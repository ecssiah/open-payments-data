HOST = 'https://openpaymentsdata.cms.gov/api/1'
ENDPOINT = '/datastore/query'

LIMIT = 500
RATE_LIMIT = 100
DELAY = 60 / RATE_LIMIT

STATES = ['NM', 'MA', 'NY', 'IL', 'MI', 'TX', 'NJ', 'PA', 'AZ', 'WA']

DATASET = '2022_GENERAL'

DATASET_IDS = {
    '2022_GENERAL': '66dfcf9a-2a9e-54b7-a0fe-cae3e42f3e8f',
    '2021_GENERAL': '0e4bd5b3-eb80-57b3-9b49-3db89212d7c5',
    '2020_GENERAL': 'e51be53b-ed10-5fa5-819b-7c2474fbdea9',
    '2019_GENERAL': '5e08488e-eadd-5e82-a4f0-01a540ea0917',
    '2018_GENERAL': 'd6a4c192-42c9-5f36-85eb-4ab2f16bb8da',
    '2017_GENERAL': 'fd7e68cb-8e96-516d-817a-ab42c022ffd3',
    '2016_GENERAL': '02ed78a8-85e9-53a3-b1ec-2869cfc236fd',
}

FIELDS = [
    'covered_recipient_type',
    'covered_recipient_profile_id',
    'covered_recipient_npi',
    'covered_recipient_first_name',
    'covered_recipient_middle_name',
    'covered_recipient_last_name',
    'covered_recipient_name_suffix',
    'recipient_primary_business_street_address_line1',
    'recipient_primary_business_street_address_line2',
    'recipient_city',
    'recipient_state',
    'recipient_zip_code',
    'covered_recipient_primary_type_1',
    'submitting_applicable_manufacturer_or_applicable_gpo_name',
    'applicable_manufacturer_or_applicable_gpo_making_payment_id',
    'applicable_manufacturer_or_applicable_gpo_making_payment_name',
    'applicable_manufacturer_or_applicable_gpo_making_payment_state',
    'applicable_manufacturer_or_applicable_gpo_making_payment_country',
    'total_amount_of_payment_usdollars',
    'date_of_payment',
    'nature_of_payment_or_transfer_of_value',
    'indicate_drug_or_biological_or_device_or_medical_supply_1',
    'product_category_or_therapeutic_area_1',
    'name_of_drug_or_biological_or_device_or_medical_supply_1',
]
