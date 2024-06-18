import pandas as pd
import datetime

#cols = instance_num,

#meta_data = provider_id, start_date, patient_num, project_name (MIGHT NOT BE APPLICABLE HERE..

id_vars = ['ent', 'dose_status', 'source', 'rule', 'offset', 'version', 'index', 'contact_date',
           'visit_id', 'enact_id', 'INSTANCE_NUM']

def enact_transform(dtc: pd.DataFrame):
    print(dtc)
    data_keys = dtc.columns
    if "project_name" not in data_keys:
        project_name = input("Missing project name! What is the project for this run?")
    else:
        project_name = data_keys['project_name']
    dtc = dtc.sort_values(['rule', 'offset'])
    dtc['INSTANCE_NUM'] = dtc.groupby(['index', 'rule']).cumcount() + 1
    transform = pd.melt(dtc, id_vars=id_vars, value_vars=['certainty', 'status', 'experiencer'],
                        var_name='MODIFIER_CD', value_name='modifier')
    transform.rename(columns={"rule":"CONCEPT_CD","contact_date":"START_DATE",
          "enact_id":"PATIENT_NUM",
          "version":"SOURCESYSTEM_CD",
          "offset":"OBSERVATION_BLOB",
          "index": "ENCOUNTER_NUM"}, inplace=True)
    if "provider_id" not in data_keys:
        transform["PROVIDER_ID"] = "@"
    transform['MODIFIER_CD'] = 'NLP|' + transform['MODIFIER_CD'] + ':' + transform['modifier']
    transform['CONCEPT_CD'] = f'NLP|CUSTOM|{project_name}:' + transform['CONCEPT_CD']
    transform["UPDATE_DATE"] = datetime.datetime.now()
    transform["VALTYPE_CD"] = None
    transform["TVAL_CHAR"] = None
    transform["NVAL_NUM"] = None
    transform["VALUEFLAG_CD"] = None
    transform["QUANTITY_NUM"] = None
    transform["UNITS_CD"] = None
    transform["END_DATE"] = None
    transform["UPLOAD_ID"] = None
    transform["IMPORT_DATE"] = None
    transform["DOWNLOAD_DATE"] = None
    transform["CONFIDENCE_NUM"] = None
    transform["LOCATION_CD"] = None
    transform.drop(columns=['modifier', 'ent', 'dose_status', 'source', 'visit_id'], inplace=True)
    print(transform)
    transform.to_csv("helper.csv", index=False)
    return transform
