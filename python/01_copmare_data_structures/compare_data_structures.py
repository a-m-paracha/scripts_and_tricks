############################################################################################
################## Author: Ayyaz Mahmood Paracha ###########################################
#################### Date: 17th May 2021 ###################################################
#################### COMPARE DATABSE #######################################################
############################################################################################
import os, arcpy, xlwt, configparser, logging, time, sys
from sqlalchemy import DDL
from matplotlib.ft2font import LOAD_IGNORE_GLOBAL_ADVANCE_WIDTH
configFile = configparser.ConfigParser()
timeStart = time.time()
configFile.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.ini"))
source_egdb = configFile.get('default', 'source_egdb')
target_egdb = configFile.get('default', 'target_egdb')
generated_report = configFile.get('default', 'generated_report')
scriptDir = os.path.dirname(os.path.realpath(__file__))
log_file_name = "Data_Comparison" + str(timeStart) + ".txt"
log_file = os.path.join(scriptDir,log_file_name)
########## Setting the handlers, which set what happens with the log messeges #######
handlers = [logging.FileHandler(log_file), logging.StreamHandler()]
# Configuring logging settings
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S',
    level=logging.INFO,
    handlers=handlers)

logging.info('The Data Compare operation Started at {}'.format(time.strftime("%Hh%Mm%Ss",time.gmtime(timeStart))))
logging.info('The script is executed from path --> ' + scriptDir)
logging.info("Source Work Space --> " + source_egdb)
logging.info("Target Work Space --> " + target_egdb)
full_generated_report = os.path.join(scriptDir, str(timeStart) + "_" +generated_report)
logging.info("Comparison report will be generated at --> " + full_generated_report)
##if(not arcpy.Exists(source_egdb)):
if(not os.path.exists(source_egdb)):
    logging.error("Source SDE File does not exist. Please verify the path. Exitting the too now!!")
    sys.exit()
else:
    logging.info("Source SDE file exists!!!")
##if(not arcpy.Exists(target_egdb)):
if(not os.path.exists(target_egdb)):
    logging.error("Target SDE File does not exist. Please verify the path. Exitting the too now!!")
    sys.exit()
else:
    logging.info("Target SDE file exists!!!")
logging.info("Pre requisites check is completed")
logging.info("--------------------------------------------------------")
logging.info("Read Connection Parameters")
source_desc = arcpy.Describe(source_egdb)
source_cp = source_desc.connectionProperties
sourceUser = ''
sourceAuthentionmode = source_cp.authentication_mode
if source_cp.authentication_mode=='OSA':
    sourceUser = 'OSUser'##cp.user
else:
    sourceUser = source_cp.user
sourceServer = source_cp.server
sourceInstance = source_cp.instance
sourceDatabase = "NOT KNOWN"
if 'oracle' not in sourceInstance:
    sourceDatabase = source_cp.database
else:
    sourceDatabase = "Oracle Database"
sourceVersion = source_cp.version

logging.info("Source Workspace Connection Parameters are")
logging.info("Source Server --> " + sourceServer)
logging.info("Source Authentication Mode --> "+ sourceAuthentionmode)
logging.info("Source User --> "+ sourceUser)
logging.info("Source Instance --> "+sourceInstance)
logging.info("Source Database --> "+sourceDatabase)
logging.info("Source Version --> " + sourceVersion)

logging.info("Target Workspace Connection Parameters are")
target_desc = arcpy.Describe(target_egdb)
target_cp = target_desc.connectionProperties
targetUser = ''
targetAuthentionmode = target_cp.authentication_mode
if target_cp.authentication_mode=='OSA':
    targetUser = 'OSUser'
else:
    targetUser = target_cp.user
targetServer = target_cp.server
targetInstance = target_cp.instance
targetDatabase = "NOT KNOWN"
if 'oracle' not in targetDatabase:
    targetDatabase = target_cp.database
else:
    targetDatabase = "Oracle Database"
targetVersion = target_cp.version

logging.info("Target Server --> " + targetServer)
logging.info("Target Authentication Mode --> " + targetAuthentionmode)
logging.info("Target User --> " + targetUser)
logging.info("Target Instance --> " + targetInstance)
logging.info("Target Database --> " + targetDatabase)
logging.info("Target Version --> " + targetVersion)
logging.info("--------------------------------------------------------")
logging.info("Excel file structure is started")
wb = xlwt.Workbook() # create empty workbook object
tabsheet_db_objects = wb.add_sheet('Database Objects')
tabsheet_db_fields = wb.add_sheet('field_comparison')

row_index_object =0
row_index_FieldMappings =0
 ####################### Columns for Feature classes, Tables and Feature Datasets ####################
col_SourceDatabase = 0
col_TargetDatabase = 1
col_SourceServer =2
col_TargetServer =3
col_SourceUser = 4
col_TargetUser = 5
col_SourceInstance = 6
col_TargetInstance = 7
col_SourceVersion = 8
col_TargetVersion = 9
col_ObjectType=10
col_SourceName=11
col_TargetName=12
col_ActionScript = 13
col_SourceRecordCount = 14
col_TargetRecordCount = 15
col_IsSourceVersion = 16
col_IsTargetVersion = 17
col_SourceArchive = 18
col_TargetArchive = 19
col_SourceEditorTracking = 20
col_TargetEditorTracking = 21
col_SourceGlobalId = 22
col_TargetGlobalId = 23
col_SourceFullPath=24
col_TargetFullPath=25

################################ Columns for Field comparison ################################
col_SourceTableName=0
col_TargetTableName=1
col_SourceFieldName=2
col_TargetFieldName=3
col_SourceFieldType=4
col_TargetFieldType=5
col_FieldNameMatch=6
col_FieldAliasMatch=7
col_FieldTypeMatch=8
col_IsEditableMatch=9
col_RequiredMatch=10
col_ScaleMatch=11
col_PrecisionMatch=12
col_IsNullableMatch=13
col_DomainMatch=14
col_DefaultMatch=15
col_BaseNameMatch=16



def CheckGlobalIds(SourceFeatureClass,TargetFeatureClass):
    descSource = arcpy.Describe(SourceFeatureClass)
    descTarget = arcpy.Describe(TargetFeatureClass)
    return descSource.hasGlobalID, descTarget.hasGlobalID

def CheckandCreateVersion(SourceFeatureClass,TargetFeatureClass):
    descSource = arcpy.Describe(SourceFeatureClass)
    descTarget = arcpy.Describe(TargetFeatureClass)
    return descSource.isVersioned, descTarget.isVersioned

def CheckandArchived(SourceFeatureClass,TargetFeatureClass):
    descSource = arcpy.Describe(SourceFeatureClass)
    descTarget = arcpy.Describe(TargetFeatureClass)
    return descSource.isArchived, descTarget.isArchived

def CheckandEnableEditorTracking(SourceFeatureClass,TargetFeatureClass):
    descSource = arcpy.Describe(SourceFeatureClass)
    descTarget = arcpy.Describe(TargetFeatureClass)
    return descSource.editorTrackingEnabled, descTarget.editorTrackingEnabled

def Created_Tables(sourceDatabase, targetDatabase, sourceServer, targetServer, 
                   sourceUser, targetUser, sourceInstance, targetInstance, sourceVersion, targetVersion, 
                   Data_Types, Source_Name, Target_Name, Action, sourcerecordcount, targetrecordcount,
                   isSourceVersioned, isTargetVersioned, isSourceArchived, isTargetArchived, 
                   isSourceTracked, isTargetTracked, SourcehasGlobalId,TargethasGlobalId,
                   Source_Object_Full_Path, Target_Object_Full_Path, row_index):
    tabsheet_db_objects.write(row_index,col_SourceDatabase, sourceDatabase)
    tabsheet_db_objects.write(row_index,col_TargetDatabase, targetDatabase)
    tabsheet_db_objects.write(row_index,col_SourceServer, sourceServer)
    tabsheet_db_objects.write(row_index,col_TargetServer, targetServer)
    tabsheet_db_objects.write(row_index,col_SourceUser, sourceUser)
    tabsheet_db_objects.write(row_index,col_TargetUser, targetUser)
    tabsheet_db_objects.write(row_index,col_SourceInstance, sourceInstance)
    tabsheet_db_objects.write(row_index,col_TargetInstance, targetInstance)
    tabsheet_db_objects.write(row_index,col_SourceVersion, sourceVersion)
    tabsheet_db_objects.write(row_index,col_TargetVersion, targetVersion)
    tabsheet_db_objects.write(row_index,col_ObjectType, Data_Types)
    tabsheet_db_objects.write(row_index,col_SourceName, Source_Name)
    tabsheet_db_objects.write(row_index,col_TargetName, Target_Name)
    tabsheet_db_objects.write(row_index,col_ActionScript, Action)
    tabsheet_db_objects.write(row_index,col_SourceRecordCount, sourcerecordcount)
    tabsheet_db_objects.write(row_index,col_TargetRecordCount, targetrecordcount)
    tabsheet_db_objects.write(row_index,col_IsSourceVersion, isSourceVersioned)
    tabsheet_db_objects.write(row_index,col_IsTargetVersion, isTargetVersioned)
    tabsheet_db_objects.write(row_index,col_SourceArchive, isSourceArchived)
    tabsheet_db_objects.write(row_index,col_TargetArchive, isTargetArchived)
    tabsheet_db_objects.write(row_index,col_SourceEditorTracking, isSourceTracked)
    tabsheet_db_objects.write(row_index,col_TargetEditorTracking, isTargetTracked)
    tabsheet_db_objects.write(row_index,col_SourceGlobalId, SourcehasGlobalId)
    tabsheet_db_objects.write(row_index,col_TargetGlobalId, TargethasGlobalId)
    tabsheet_db_objects.write(row_index,col_SourceFullPath, Source_Object_Full_Path)
    tabsheet_db_objects.write(row_index,col_TargetFullPath, Target_Object_Full_Path)
    wb.save(full_generated_report)

def Field_Comparison(Source_Table_Name, Target_Table_Name, Source_Field_Name, Target_Field_Name, 
                    Source_Field_Type, Target_Field_Type, Field_Name_Match, Field_Alias_Match,
                    Field_Type_Match,Field_Is_Editable_Match, Field_Is_Required_Match,
                    Field_Scale_Match,Field_Precision_Match,Field_Is_Nullable_Match,Field_Has_Domain_Match,
                    Field_Has_Default_Match, Field_Has_Base_Name_Match, row_index_FieldMappings):
    tabsheet_db_fields.write(row_index_FieldMappings,col_SourceTableName, Source_Table_Name)
    tabsheet_db_fields.write(row_index_FieldMappings,col_TargetTableName, Target_Table_Name)
    tabsheet_db_fields.write(row_index_FieldMappings,col_SourceFieldName, Source_Field_Name)
    tabsheet_db_fields.write(row_index_FieldMappings,col_TargetFieldName, Target_Field_Name)
    tabsheet_db_fields.write(row_index_FieldMappings,col_SourceFieldType, Source_Field_Type)
    tabsheet_db_fields.write(row_index_FieldMappings,col_TargetFieldType, Target_Field_Type)
    tabsheet_db_fields.write(row_index_FieldMappings,col_FieldNameMatch, Field_Name_Match)
    tabsheet_db_fields.write(row_index_FieldMappings,col_FieldAliasMatch, Field_Alias_Match)
    tabsheet_db_fields.write(row_index_FieldMappings,col_FieldTypeMatch, Field_Type_Match)
    tabsheet_db_fields.write(row_index_FieldMappings,col_IsEditableMatch, Field_Is_Editable_Match)
    tabsheet_db_fields.write(row_index_FieldMappings,col_RequiredMatch, Field_Is_Required_Match)
    tabsheet_db_fields.write(row_index_FieldMappings,col_ScaleMatch, Field_Scale_Match)
    tabsheet_db_fields.write(row_index_FieldMappings,col_PrecisionMatch, Field_Precision_Match)
    tabsheet_db_fields.write(row_index_FieldMappings,col_IsNullableMatch, Field_Is_Nullable_Match)
    tabsheet_db_fields.write(row_index_FieldMappings,col_DomainMatch, Field_Has_Domain_Match)
    tabsheet_db_fields.write(row_index_FieldMappings,col_DefaultMatch, Field_Has_Default_Match)
    tabsheet_db_fields.write(row_index_FieldMappings,col_BaseNameMatch, Field_Has_Base_Name_Match)
    wb.save(full_generated_report)

def CompareGenerateFieldMappings(SourceFeatureClass, TargetFeatureClass, row_index_FieldMappings):
    logging.info("Fields are being matched")
    fieldsSource = arcpy.ListFields(SourceFeatureClass)
    fieldsTarget = arcpy.ListFields(TargetFeatureClass)
    for fieldSource in fieldsSource:
        Matchfound = False
        for fieldTarget in fieldsTarget:
            if fieldSource.name.lower()==fieldTarget.name.lower():
                Matchfound = True
                tabsheet_db_fields.write(row_index_FieldMappings,col_SourceTableName, os.path.basename(SourceFeatureClass))
                tabsheet_db_fields.write(row_index_FieldMappings,col_TargetTableName, os.path.basename(TargetFeatureClass))
                tabsheet_db_fields.write(row_index_FieldMappings,col_SourceFieldName, fieldSource.name)
                tabsheet_db_fields.write(row_index_FieldMappings,col_TargetFieldName, fieldTarget.name)
                tabsheet_db_fields.write(row_index_FieldMappings,col_SourceFieldType, fieldSource.type)
                tabsheet_db_fields.write(row_index_FieldMappings,col_TargetFieldType, fieldTarget.type)
                tabsheet_db_fields.write(row_index_FieldMappings,col_FieldNameMatch, True)
                if fieldSource.aliasName==fieldTarget.aliasName:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_FieldAliasMatch, True)
                else:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_FieldAliasMatch, False)
                if fieldSource.type==fieldTarget.type:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_FieldTypeMatch, True)
                else:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_FieldTypeMatch, False)                            
                if fieldSource.editable==fieldTarget.editable:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_IsEditableMatch, True)
                else:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_IsEditableMatch, False)                            
                if fieldSource.required==fieldTarget.required:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_RequiredMatch, True)
                else:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_RequiredMatch, False)                            
                if fieldSource.scale==fieldTarget.scale:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_ScaleMatch, True)
                else:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_ScaleMatch, False)                                                   
                if fieldSource.precision==fieldTarget.precision:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_PrecisionMatch, True)
                else:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_PrecisionMatch, False)                                 
                if fieldSource.isNullable==fieldTarget.isNullable:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_IsNullableMatch, True)
                else:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_IsNullableMatch, False)                                 
                if fieldSource.domain==fieldTarget.domain:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_DomainMatch, True)
                else:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_DomainMatch, False)                                
                if fieldSource.defaultValue==fieldTarget.defaultValue:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_DefaultMatch, True)
                else:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_DefaultMatch, False)                               
                if fieldSource.baseName==fieldTarget.baseName:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_BaseNameMatch, True)
                else:
                    tabsheet_db_fields.write(row_index_FieldMappings,col_BaseNameMatch, False)
                row_index_FieldMappings = row_index_FieldMappings + 1
                wb.save(full_generated_report)
        if not Matchfound:
            tabsheet_db_fields.write(row_index_FieldMappings,col_SourceTableName, os.path.basename(SourceFeatureClass))
            tabsheet_db_fields.write(row_index_FieldMappings,col_TargetTableName, os.path.basename(TargetFeatureClass))
            tabsheet_db_fields.write(row_index_FieldMappings,col_SourceFieldName, fieldSource.name)
            tabsheet_db_fields.write(row_index_FieldMappings,col_FieldNameMatch, False)
            row_index_FieldMappings = row_index_FieldMappings + 1
            wb.save(full_generated_report)
    for fieldTarget in fieldsTarget:
        Matchfound = False
        for fieldSource in fieldsSource:
            if fieldSource.name.lower()==fieldTarget.name.lower():
                Matchfound = True
        if not Matchfound:
            tabsheet_db_fields.write(row_index_FieldMappings,col_SourceTableName, os.path.basename(SourceFeatureClass))
            tabsheet_db_fields.write(row_index_FieldMappings,col_TargetTableName, os.path.basename(TargetFeatureClass))
            tabsheet_db_fields.write(row_index_FieldMappings,col_TargetFieldName, fieldTarget.name)
            tabsheet_db_fields.write(row_index_FieldMappings,col_FieldNameMatch, False)
            row_index_FieldMappings = row_index_FieldMappings + 1
            wb.save(full_generated_report)

    return row_index_FieldMappings

Created_Tables('Source Database', 'Target Database', 'Source Server', 'Target Server', 
               'Source User', 'Target User', 'SourceInstance', 'Target Instance', 'Source Version', 'Target Version',
               'Data Types', 'Source Name', 'Target Name', 'Action', 'Source Record Count', 'Target Record Count',
               'Is Source Versioned', 'Is Target Versioned', 'Is Source Archived', 'Is Target Archived',
               'Source Editor Tracking Enabled', 'Target Editor Tracking Enabled', 'Source Has GlobalIds', 'Target Has GlobalIds',
               'Source Object Full Path', 'Target Object Full Path', row_index_object)
row_index_object = row_index_object + 1

Field_Comparison('Source Table Name', 'Target Table Name', 'Source Field Name', 'Target Field Name', 
                'Source Field Type', 'Target Field Type', 'Field Name Match', 'Field Alias Match', 
                'Field Type Match', 'Field Is Editable Match', 'Field Is Required Match', 'Field Scale Match',
                'Field Precision Match', 'Field Is Nullable Match', 'Field Has Domain Match', 
                'Field Has Default Match', 'Field Has Base Name Match', row_index_FieldMappings)
row_index_FieldMappings = row_index_FieldMappings + 1

logging.info("Excel file structure is completed")
logging.info("--------------------------------------------------------")
logging.info("Analyzing Feature Classes")
arcpy.env.workspace= source_egdb
datasets = []
datasets = arcpy.ListDatasets(feature_type='feature')
datasets = [''] + datasets if datasets is not None else []
for ds in datasets:
    if ds == '':
        for fc in arcpy.ListFeatureClasses(feature_dataset=ds):
            logging.info("The system is analyzing feature class --> " + fc)
            if sourceDatabase == "Oracle Database":
                schema, fcName = fc.split(".")
            else:
                db, schema, fcName = fc.split(".")
            SourceRecordCount = arcpy.GetCount_management(fc)[0]
            if targetDatabase == "Oracle Database":
                targetFeatureClass = os.path.join(target_egdb, targetUser + "." +  fcName)
            else:
                targetFeatureClass = os.path.join(target_egdb, targetDatabase + "." + targetUser + "." +  fcName)
            
            if arcpy.Exists(targetFeatureClass):
                logging.info("Check Target Record Count")
                TargetRecordCount = arcpy.GetCount_management(targetFeatureClass)[0]
                logging.info("Check Target Global IDs Configuration")
                CheckGlobalIdsValues = CheckGlobalIds(fc,targetFeatureClass)
                logging.info("Check Target Version")
                CheckCreateVersion = CheckandCreateVersion(fc,targetFeatureClass)
                logging.info("Check Target Archived")
                CheckArchived = CheckandArchived(fc,targetFeatureClass)
                logging.info("Check Target Enable Editor Tracking")
                CheckEnableEditorTracking = CheckandEnableEditorTracking(fc,targetFeatureClass)
                Created_Tables(sourceDatabase, targetDatabase, sourceServer, targetServer, 
                                sourceUser, targetUser, sourceInstance, targetInstance, sourceVersion, targetVersion, 
                                'Featuer Class', fc, os.path.basename(targetFeatureClass), 
                                'Exists in Source and Target', str(SourceRecordCount),str(TargetRecordCount),
                                str(CheckCreateVersion[0]),str(CheckCreateVersion[1]),
                                str(CheckArchived[0]),str(CheckArchived[1]),
                                str(CheckEnableEditorTracking[0]),str(CheckEnableEditorTracking[1]),
                                str(CheckGlobalIdsValues[0]),str(CheckGlobalIdsValues[1]), 
                                os.path.join(source_egdb, fc), targetFeatureClass, row_index_object)
                row_index_object = row_index_object + 1
                row_index_FieldMappings = CompareGenerateFieldMappings(fc, targetFeatureClass, row_index_FieldMappings)
            else:
                Created_Tables(sourceDatabase, targetDatabase, sourceServer, targetServer, 
                    sourceUser, targetUser, sourceInstance, targetInstance, sourceVersion, targetVersion, 
                    'Featuer Class', fc, "", 
                    'DOES NOT EXIST IN TARGET', "","","","","","","","","", "","", row_index_object)
                row_index_object = row_index_object + 1
            logging.info("--------------------------------------------------------")
    else:
        logging.info("DS exists and value is --> " + ds)
        if sourceDatabase == "Oracle Database":
            schema, dsName = ds.split(".")
        else:
            db, schema, dsName = ds.split(".")
        if targetDatabase == "Oracle Database":
            target_feature_dataset = os.path.join(target_egdb, targetUser + "." +  dsName)
        else:
            target_feature_dataset = os.path.join(target_egdb, targetDatabase + "." + targetUser + "." +  dsName)

        if  arcpy.Exists(target_feature_dataset):
            Created_Tables(sourceDatabase, targetDatabase, sourceServer, targetServer, 
                sourceUser, targetUser, sourceInstance, targetInstance, sourceVersion, targetVersion, 
                'Feature DataSet', ds,
                os.path.basename(target_feature_dataset), 'Exists in Source and Target','','',
                '','','','','','','','',
                os.path.join(source_egdb, ds), 
                os.path.join(target_egdb, target_feature_dataset),row_index_object)
            row_index_object = row_index_object + 1 
            for fc in arcpy.ListFeatureClasses(feature_dataset=ds):
                logging.info("The system is analyzing feature class --> " + fc)
                if sourceDatabase == "Oracle Database":
                    schema, fcName = fc.split(".")
                else:
                    db, schema, fcName = fc.split(".")
                SourceRecordCount = arcpy.GetCount_management(fc)[0]
                if targetDatabase == "Oracle Database":
                    targetFeatureClass = os.path.join(target_feature_dataset, targetUser + "." +  fcName)
                else:
                    targetFeatureClass = os.path.join(target_feature_dataset, targetDatabase + "." + targetUser + "." +  fcName)
                if arcpy.Exists(targetFeatureClass):
                    logging.info("Check Target Record Count")
                    TargetRecordCount = arcpy.GetCount_management(targetFeatureClass)[0]
                    logging.info("Check Target Global IDs Configuration")
                    CheckGlobalIdsValues = CheckGlobalIds(fc,targetFeatureClass)
                    logging.info("Check Target Version")
                    CheckCreateVersion = CheckandCreateVersion(fc,targetFeatureClass)
                    logging.info("Check Target Archived")
                    CheckArchived = CheckandArchived(fc,targetFeatureClass)
                    logging.info("Check Target Enable Editor Tracking")
                    CheckEnableEditorTracking = CheckandEnableEditorTracking(fc,targetFeatureClass)
                    Created_Tables(sourceDatabase, targetDatabase, sourceServer, targetServer, 
                                sourceUser, targetUser, sourceInstance, targetInstance, sourceVersion, targetVersion, 
                                'Featuer Class', fc, os.path.basename(targetFeatureClass), 
                                'Exists in Source and Target', str(SourceRecordCount),str(TargetRecordCount),
                                str(CheckCreateVersion[0]),str(CheckCreateVersion[1]),
                                str(CheckArchived[0]),str(CheckArchived[1]),
                                str(CheckEnableEditorTracking[0]),str(CheckEnableEditorTracking[1]),
                                str(CheckGlobalIdsValues[0]),str(CheckGlobalIdsValues[1]), 
                                os.path.join(source_egdb, fc), targetFeatureClass, row_index_object)
                    row_index_object = row_index_object + 1
                    row_index_FieldMappings = CompareGenerateFieldMappings(fc, targetFeatureClass, row_index_FieldMappings)
                else:
                    Created_Tables(sourceDatabase, targetDatabase, sourceServer, targetServer, 
                        sourceUser, targetUser, sourceInstance, targetInstance, sourceVersion, targetVersion, 
                        'Featuer Class', fc, "", 
                        'DOES NOT EXIST IN TARGET', "","","","","","","","","", "","", row_index_object)
                    row_index_object = row_index_object + 1
                logging.info("--------------------------------------------------------")
        else:
            Created_Tables(sourceDatabase, targetDatabase, sourceServer, targetServer, 
                sourceUser, targetUser, sourceInstance, targetInstance, sourceVersion, targetVersion, 
                'Featuer DataSet', fc, "", 
                'DOES NOT EXIST IN TARGET', "","","","","","","","","",                 
                os.path.join(source_egdb, ds), 
                os.path.join(target_egdb, target_feature_dataset), row_index_object)
            row_index_object = row_index_object + 1           
        logging.info("--------------------------------------------------------")
logging.info("Feature classes are completely analyzed")
logging.info("--------------------------------------------------------")
logging.info("Analyzing Tables")
tables = []
tables = arcpy.ListTables()
for tab in tables:
    
    logging.info("The system is analyzing table --> " + tab)
    if sourceDatabase == "Oracle Database":
        schema, tabName = tab.split(".")
    else:
        db, schema, tabName = tab.split(".")

    SourceRecordCount = arcpy.GetCount_management(tab)[0]
    if targetDatabase == "Oracle Database":
        targetTable = os.path.join(target_egdb, targetUser + "." +  tabName)
    else:
        targetTable = os.path.join(target_egdb, targetDatabase + "." + targetUser + "." +  tabName)

    if arcpy.Exists(targetTable):
        logging.info("Check Target Record Count")
        TargetRecordCount = arcpy.GetCount_management(targetTable)[0]
        logging.info("Check Target Global IDs Configuration")
        CheckGlobalIdsValues = CheckGlobalIds(tab,targetTable)
        logging.info("Check Target Version")
        CheckCreateVersion = CheckandCreateVersion(tab,targetTable)
        logging.info("Check Target Archived")
        CheckArchived = CheckandArchived(tab,targetTable)
        logging.info("Check Target Enable Editor Tracking")
        CheckEnableEditorTracking = CheckandEnableEditorTracking(tab,targetTable)
        Created_Tables(sourceDatabase, targetDatabase, sourceServer, targetServer, 
            sourceUser, targetUser, sourceInstance, targetInstance, sourceVersion, targetVersion, 
            'Table', tab, os.path.basename(targetTable), 
            'Exists in Source and Target', str(SourceRecordCount),str(TargetRecordCount),
            str(CheckCreateVersion[0]),str(CheckCreateVersion[1]),
            str(CheckArchived[0]),str(CheckArchived[1]),
            str(CheckEnableEditorTracking[0]),str(CheckEnableEditorTracking[1]),
            str(CheckGlobalIdsValues[0]),str(CheckGlobalIdsValues[1]), 
            os.path.join(source_egdb, tab), targetTable, row_index_object)
        row_index_object = row_index_object + 1
        row_index_FieldMappings = CompareGenerateFieldMappings(tab, targetTable, row_index_FieldMappings)  
    else:
        Created_Tables(sourceDatabase, targetDatabase, sourceServer, targetServer, 
            sourceUser, targetUser, sourceInstance, targetInstance, sourceVersion, targetVersion, 
            'Table', tab, "", 
            'DOES NOT EXIST IN TARGET', "","","","","","","","","",                 
            os.path.join(source_egdb, tab), 
            targetTable, row_index_object)
        row_index_object = row_index_object + 1 

logging.info("Tables are completely analyzed")
logging.info("--------------------------------------------------------")
