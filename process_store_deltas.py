import arcpy
import csv
import logging
import sys
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

#Set-up logging
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
_log = logging.getLogger(__name__)
_log_fh = logging.FileHandler(os.path.join(os.path.dirname(__file__), 'store_process.log'))
_log_fh.setFormatter(formatter)
_log_fh.setLevel(logging.INFO)
_log.addHandler(_log_fh)

#Set-up the GP environment
arcpy.env.qualifiedFieldNames = False;
arcpy.env.overwriteOutput = True;
arcpy.env.scratchWorkspace = os.path.join(os.path.dirname(__file__), 'scratch.gdb')

def projectGeocodeResult(workspace,inputFeatureClass,outputFeatureClass, outCoordinateSystemString):
    try:
        # Use ListFeatureClasses to generate a list of inputs
        for inputFeatureClass in arcpy.ListFeatureClasses():

            # Determine if the input has a defined coordinate system, can't project it if it does not
            description = arcpy.Describe(inputFeatureClass)

            if description.spatialReference.Name == "Unknown":
                _log.info('Skipped this feature class due to undefined coordinate system: ' + inputFeatureClass)

            else:
                # Determine the new output feature class path and name
                outputFeatureClass = os.path.join(workspace, inputFeatureClass)

                # Set output coordinate system
                outCS = arcpy.SpatialReference(outCoordinateSystemString)

                # run project tool
                arcpy.Project_management(inputFeatureClass, outputFeatureClass, outCS)

                # check messages
                _log.info(arcpy.GetMessages())


    except arcpy.ExecuteError:
        print(arcpy.GetMessages(2))

    except Exception as ex:
        print(ex.args[0])

    return ""

def process_store_facts(args):
    logging.basicConfig(level=logging.DEBUG)
    _log.info('Process stores and facts started')

    #Set-up inputs
    store_fc = os.path.join(os.path.dirname(__file__),'RBAMRGIS03.sde/RBGIS01.DBO.s_gis_store_pnt')
    #store_facts_month = os.path.join(os.path.dirname(__file__),'RBAMRGIS03.sde/RBGIS01.dbo.f_gis_month_store')
    store_facts_year = os.path.join(os.path.dirname(__file__),'RBAMRGIS03.sde/RBGIS01.dbo.f_gis_year_store')
    store_deltas =  os.path.join(os.path.dirname(__file__),'RBAMRGIS03.sde/RBGIS01.dbo.d_gis_store_delta')
    locator = args[1]

    #Set-up outputs
    geocode_result = arcpy.env.scratchWorkspace + '/geocode_result'
    geocode_projected = arcpy.env.scratchWorkspace + '/stores_projected'
    deltas_view_new = 'deltas_new'
    deltas_table_new = arcpy.env.scratchWorkspace + '/select_new_result'
    deltas_view_removed = 'deltas_removed'
    deltas_table_removed = arcpy.env.scratchWorkspace + '/select_remove_result'
    fact_view = 'fact_view'
    fact_table = arcpy.env.scratchWorkspace + '/fact_table'
    store_feature_layer = 'store_feature_layer'
    store_fact_result = arcpy.env.scratchWorkspace + '/store_fact_result'

    #Select new deltas
    deltas_where_new = "type = 'New'"
    arcpy.MakeQueryTable_management(store_deltas, deltas_view_new, 'NO_KEY_FIELD','','', deltas_where_new)
    deltas_new_count = int(arcpy.GetCount_management(deltas_view_new).getOutput(0))
    arcpy.CopyRows_management(deltas_view_new, deltas_table_new)
    _log.info('Created table view of store deltas where ' + deltas_where_new + ' with ' + str(deltas_new_count) + ' rows')

    #Geocode new deltas
    _log.info('Starting to geocode new store deltas')
    arcpy.GeocodeAddresses_geocoding(deltas_table_new, locator, "Address store_addr1 VISIBLE NONE;City store_city VISIBLE NONE;State state_code VISIBLE NONE;Zip zip VISIBLE NONE", geocode_result)
    _log.info('Finished geocoding new store deltas')

    try:
        #Begin append new deltas to master FC
        #Begin remove deltas from master store FC
        workspace = os.path.dirname(store_fc)

        #Set-up fields for use in search, update, and insert cursors
        store_fields = arcpy.ListFields(store_deltas)
        store_field_names = []
        for store_field in store_fields:
            if store_field.name != 'sub_channel' and store_field.name != 'store_status' and store_field.name != 'OBJECTID':
                store_field_names.append(store_field.name)
        store_fields_string = '; '.join(store_field_names)

        #Set-up where clauses
        geocodes_where_matched = ''' "Status" = 'M' OR "Status" = 'T' '''
        deltas_where_remove = "type = 'Removed'"

        #Begin insert and remove deltas with edit session
        with arcpy.da.Editor(workspace) as edit:
            #Set-up cursors for inserts
            insert_cursor = arcpy.InsertCursor(store_fc, '')
            search_cursor_geocodes = arcpy.SearchCursor(geocode_result, geocodes_where_matched, '', store_fields_string)

            _log.info('Begin to insert new store deltas into master store feature class')

            #Begin inserts
            for delta_row_new in search_cursor_geocodes:
                insert_cursor.insertRow(delta_row_new)
                _log.info('Inserted new store with store id ' + str(delta_row_new.store_id))

            _log.info('Finished inserting new store deltas into master store feature class')

            #Clean-up insert-related cursors
            del delta_row_new
            del search_cursor_geocodes
            del insert_cursor

            #Make table view to include rows that should be removed
            arcpy.MakeQueryTable_management(store_deltas, deltas_view_removed, 'NO_KEY_FIELD','','', deltas_where_remove)
            deltas_remove_count = int(arcpy.GetCount_management(deltas_view_removed).getOutput(0))
            arcpy.CopyRows_management(deltas_view_removed, deltas_table_removed)

            _log.info('Created table view of remove store deltas where ' + deltas_where_remove + ' with ' + str(deltas_remove_count) + ' rows')

            #Get IDs to delete
            search_cursor_remove = arcpy.SearchCursor(deltas_table_removed,'','', u'store_id')
            remove_ids = []
            for delta_row_remove in search_cursor_remove:
                remove_ids.append(delta_row_remove.store_id)
            #Clean-up cursor
            del delta_row_remove
            del search_cursor_remove

            _log.info('Begin to delete store deltas from master store feature class where' + deltas_where_remove)

            #Begin delete
            update_cursor = arcpy.da.UpdateCursor(store_fc, [u'store_id'])
            for row in update_cursor:
                if row[0] in remove_ids:
                    update_cursor.deleteRow()
                    _log.info('Deleted row with id ' + str(row[0]))

            #Clean-up delete-related cursor
            del row
            del update_cursor

            _log.info('Finished deleting store deltas from master store feature class')
            _log.info('Master store feature class now contains ' + str(arcpy.GetCount_management(store_fc)))

        #Project the geocodes results
        projectGeocodeResult(arcpy.env.scratchWorkspace,store_fc,geocode_projected, "WGS 1984 Web Mercator Auxiliary Sphere")
    except arcpy.ExecuteError as e:
        log_msg = 'Could not insert or remove all store deltas from master store feature class - rolling back changes'
        _log.error(log_msg)
        exception_report = Exception(log_msg + '<br>' + e.message)
        send_alert_email(exception_report)

def send_alert_email(exception):
    #Get email address from file
    file_reader = csv.DictReader(open(os.path.join(os.path.dirname(__file__), 'email_distribution_list.csv'),'rb'))
    name_email = []
    for data in file_reader:
        name_email.append(data)

    #Send message
    s = smtplib.SMTP('smtp.lan.us.redbull.com:25')
    for recipient in name_email:
        #Create message container - the correct MIME type is multipart/alternative.
        msg = MIMEMultipart('related')
        msg['Subject'] = 'Alert: Error processing store facts'
        msg['From'] = 'redbullgis@us.redbull.com'
        msg['To'] = recipient['email']

        html = """
        <html>
            <head></head>
            <body>
            <p>
                <b>""" + recipient['name'] + """:</b><br><br>
                <b>This message is to notify you that an error has occurred in processing the store facts table.</b><br><br>
                """ + exception.message + """
            </p>
            </body>
        </html>
        """

        #Record the MIME type - text/html
        part1 = MIMEText(html, 'html')
        #Attach parts into message container.
        #According to RFC 2046, the last part of a multipart message, in this case
        #the HTML message, is best and preferred.
        msg.attach(part1)

        #Send the message via local SMTP server.
        #sendmail function takes 3 arguments: sender's address, recipient's address
        #and message to send - here it is sent as one string.
        s.sendmail('redbullgis@us.redbull.com', recipient['email'], msg.as_string())
    s.quit()

if __name__ == '__main__':
    try:
        #Main function to process store deltas and join facts
        process_store_facts(sys.argv)
    except Exception as e:
        #Log exception
        _log.error(e.message)

        #Send error email
        send_alert_email(e)
