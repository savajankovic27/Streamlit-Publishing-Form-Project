import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import uuid 
import re 
from datetime import datetime

# Initialize session state (make variable global) for the app
if 'page' not in st.session_state:
    st.session_state.page = 0
if 'num_mdps' not in st.session_state:
    st.session_state.num_mdps = 0
if 'doc_number' not in st.session_state:
    st.session_state.doc_number = ""
if 'doc_details' not in st.session_state:
    st.session_state.doc_details = None
if 'num_mdu' not in st.session_state:
    st.session_state.num_mdu = {}
if 'mdp_index' not in st.session_state:
    st.session_state.mdp_index = 0

# FETCHING FUNCTIONS

def fetch_doc_details(doc_number, session):
    query = f"""
    SELECT * 
    FROM DF_ONEMESH_PFMGMT_DEV.STG_DGO.DATA_ASSET_REGISTRATION 
    WHERE ID = '{doc_number}' AND CE_IS_CATALOUGED = 'Yes' 
    """
    result = session.sql(query).to_pandas()
    return result if not result.empty else None

def fetch_terms_of_use(session):
    query = f"""
    SELECT NAME 
    FROM DF_ONEMESH_PFMGMT_DEV.STG_CDMP.TERMSOFUSE
    """
    result = session.sql(query).to_pandas()
    return result['NAME'].tolist() if not result.empty else []

def fetch_delivery_formats(session):
    query = f"""
    SELECT NAME 
    FROM DF_ONEMESH_PFMGMT_DEV.STG_CDMP.DELIVERY_FORMATS
    """
    result = session.sql(query).to_pandas()
    return result['NAME'].tolist() if not result.empty else []

def fetch_categories(session):
    query = f"""
    SELECT NAME 
    FROM DF_ONEMESH_PFMGMT_DEV.STG_CDMP.CATEGORIES
    """
    result = session.sql(query).to_pandas()
    return result['NAME'].tolist() if not result.empty else []

def fetch_data_asset_structure(session):
    query = f"""
    SELECT DATA_ASSET_STRUCTURE
    FROM DF_ONEMESH_PFMGMT_DEV.STG_DGO.DATA_ASSET_REGISTRATION
    """
    result = session.sql(query).to_pandas()
    return result['DATA_ASSET_STRUCTURE'].tolist() if not result.empty else []

def fetch_usage_context(session):
    query = f"""
    SELECT NAME
    FROM DF_ONEMESH_PFMGMT_DEV.STG_CDMP.USAGE_CONTEXT
    """
    result = session.sql(query).to_pandas()
    return result ['NAME'].tolist() if not result.empty else []

def insert_publishing_form_data():
    timestamp_created = datetime.now().strftime('%Y-%m-%d  %H:%M:%S')
    user = st.experimental_user["email"]
    # Query to get the USER_ID, properly enclosing the email in single quotes
    user_id_query = f"""
    SELECT USER_ID FROM DF_ONEMESH_PFMGMT_DEV.STG_CDMP.CONTACT_NEW WHERE CONTACT_DETAIL = '{user}'
    """
    user_id_result = session.sql(user_id_query).collect()
    # Check if the result is empty
    if not user_id_result:
        st.error("User ID not found for the provided email.")
        return None
    
    # Safely retrieve the USER_ID
    user_id = user_id_result[0]["USER_ID"]
    
    query = f"""
    INSERT INTO STG_CDMP.PUBLISHING_FORM (
        ID, PUBLISHER, DOC_TOOL_NUMBER, STATUS, USER_ID_CREATED, USER_ID_MODIFIED, TIMESTAMP_CREATED,TIMESTAMP_MODIFIED
    ) VALUES (
        '{st.session_state.form_id}', '{user}', '{st.session_state.doc_number}','Completed','{user_id}',NULL,'{timestamp_created}', NULL
    )
    """
    session.sql(query).collect()
    return st.session_state.form_id
## Form ID Generation. Used by the functions 
st.session_state.form_id = str(uuid.uuid4())

def insert_publishing_form_mdp_data():
    timestamp_created = datetime.now().strftime('%Y-%m-%d  %H:%M:%S')
    user = st.experimental_user["email"]

    user_id_query = f"""
    SELECT USER_ID FROM DF_ONEMESH_PFMGMT_DEV.STG_CDMP.CONTACT_NEW WHERE CONTACT_DETAIL = '{user}'
    """
    user_id_result = session.sql(user_id_query).collect()

    if not user_id_result:
        st.error("User ID not found for the user")
        return None

    user_id = user_id_result[0]["USER_ID"]

    mdp_ids = [str(uuid.uuid4()) for _ in range(st.session_state.num_mdps)]  # Generate the IDs

    data = {
        'ID': mdp_ids,
        'DATA_PRODUCT_NAME': st.session_state.mdp_names,
        'FORM_ID': [st.session_state.form_id] * st.session_state.num_mdps,
        'DATA_OWNERS': st.session_state.data_owner_list,
        'DESCRIPTION': st.session_state.description_list,
        'USAGE_CRITERIA': [", ".join(criteria) for criteria in st.session_state.usage_criteria_list],
        'GRANULARITY': st.session_state.data_granularity_list,
        'DATA_REFRESH_FREQUENCY': st.session_state.data_refresh_frequency_list,
        'MDP_TAGS': st.session_state.mdp_tag_list,
        'DATA_STORAGE_LOCATION_LINK': st.session_state.storage_location_list,
        'DELIVERY_FORMAT': st.session_state.delivery_formats_list,
        'AD_GROUP': st.session_state.ADoptions_list,
        'PROVIDER_ACCOUNT': [st.session_state.provider_account_list[0]] * st.session_state.num_mdps,
        'ADDITIONAL_DETAIL': st.session_state.additional_details_list,
        'USER_ID_CREATED': [user_id] * st.session_state.num_mdps,
        'USER_ID_MODIFIED': [None] * st.session_state.num_mdps,
        'TIMESTAMP_CREATED': [timestamp_created] * st.session_state.num_mdps,
        'TIMESTAMP_MODIFIED': [None] * st.session_state.num_mdps
    }

    df = pd.DataFrame(data)

    for index, row in df.iterrows():
        query = f"""
        INSERT INTO STG_CDMP.PUBLISHING_MDP (
            ID, DATA_PRODUCT_NAME, FORM_ID, DATA_OWNERS, DESCRIPTION, USAGE_CRITERIA, GRANULARITY, DATA_REFRESH_FREQUENCY, MDP_TAGS, DATA_STORAGE_LOCATION_LINK, DELIVERY_FORMAT, AD_GROUP, PROVIDER_ACCOUNT, ADDITIONAL_DETAIL, USER_ID_CREATED, USER_ID_MODIFIED, TIMESTAMP_CREATED, TIMESTAMP_MODIFIED
        ) VALUES (
            '{row.ID}', '{row.DATA_PRODUCT_NAME}', '{row.FORM_ID}', '{row.DATA_OWNERS}', '{row.DESCRIPTION}', '{row.USAGE_CRITERIA}', '{row.GRANULARITY}', '{row.DATA_REFRESH_FREQUENCY}', '{row.MDP_TAGS}', '{row.DATA_STORAGE_LOCATION_LINK}', '{row.DELIVERY_FORMAT}', '{row.AD_GROUP}', '{row.PROVIDER_ACCOUNT}', '{row.ADDITIONAL_DETAIL}', '{row.USER_ID_CREATED}', NULL, '{row.TIMESTAMP_CREATED}', NULL
        )
        """
        session.sql(query).collect()

    st.session_state.mdp_id = mdp_ids  # Store the generated IDs in session state



def insert_publishing_form_mdu_data():
    timestamp_created = datetime.now().strftime('%Y-%m-%d  %H:%M:%S')
    user = st.experimental_user["email"]
    # Query to get the USER_ID, properly enclosing the email in single quotes
    user_id_query = f"""
    SELECT USER_ID FROM DF_ONEMESH_PFMGMT_DEV.STG_CDMP.CONTACT_NEW WHERE CONTACT_DETAIL = '{user}'
    """
    user_id_result = session.sql(user_id_query).collect()
    # Check if the result is empty
    if not user_id_result:
        st.error("User ID not found for the provided email.")
        return None
    mdu_id = str(uuid.uuid4())
    # Safely retrieve the USER_ID
    user_id = user_id_result[0]["USER_ID"]

    for i in range(st.session_state.num_mdps):
        for j in range (st.session_state.num_mdu):
            query = f"""
            INSERT INTO STG_CDMP.PUBLISHING_MDU (
                ID, FORM_ID, PUBLISHING_DATA_ASSETS_CDGC_LINK, MDP_NAME, USER_ID_CREATED, USER_ID_MODIFIED, TIMESTAMP_CREATED,TIMESTAMP_MODIFIED
            ) VALUES (
                '{mdu_id}','{st.session_state.form_id}', '{st.session_state.dataset_links[j]}', '{st.session_state.mdp_names[i]}','{user_id}',NULL,'{timestamp_created}', NULL
            )
            """
    session.sql(query).collect()
    
def fetch_mdp_data(mdp_id, session):
    query = f"""
    SELECT 
        DATA_PRODUCT_NAME,
        DATA_OWNERS,
        DESCRIPTION,
        USAGE_CRITERIA,
        GRANULARITY,
        DATA_REFRESH_FREQUENCY,
        MDP_TAGS,
        DATA_STORAGE_LOCATION_LINK,
        DELIVERY_FORMAT,
        AD_GROUP,
        PROVIDER_ACCOUNT,
        ADDITIONAL_DETAIL
    FROM 
        STG_CDMP.PUBLISHING_MDP
    WHERE 
        ID = '{mdp_id}'
    """
    
    result = session.sql(query).to_pandas()

    if result.empty:
        st.warning("No MDP found with the provided ID.")
        return None
    
    # Store the fetched data in session state for prefilling fields
    st.session_state.mdp_data = result.iloc[0].to_dict()

    # Display a success message
    st.success("MDP data found successfully!")

    # Display the DataFrame to the user
    st.dataframe(result)
    
    return result
    
# MAIN APP 
st.title("OneMesh Publishing Form")
session = get_active_session()

if 'terms_of_use' not in st.session_state or not st.session_state.terms_of_use:
    st.session_state.terms_of_use = fetch_terms_of_use(session)
    
if 'delivery_formats' not in st.session_state or not st.session_state.delivery_formats:
    st.session_state.delivery_formats = fetch_delivery_formats(session)

if 'categories' not in st.session_state or not st.session_state.categories:
    st.session_state.categories = fetch_categories(session)

if 'usage_context' not in st.session_state or not st.session_state.usage_context:
    st.session_state.usage_context = fetch_usage_context(session)

if 'data_owners' not in st.session_state:
    st.session_state.data_owners = []

# PAGE 0
if st.session_state.page == 0:
    st.write("This form is for initiating the process to publish data sets to OneMesh Marketplace.")
    st.markdown("""
    ### Please enter the DOC# for your data asset's registration in Data One Classifier:
    """)
    
    st.session_state.doc_number = st.text_input("*FOR TESTING PURPOSE ONLY: DOC NUMBER EXAMPLE IS 95 FOR STRUC. OR 99 FOR SEMI-STRUC.*")

    if st.button('Check'):
        if not st.session_state.doc_number:
            st.error("Please enter a number")
        else:
            try:
                doc_details = fetch_doc_details(st.session_state.doc_number, session)
                if doc_details is not None:
                    st.session_state.doc_details = doc_details
                    st.session_state.page = 1
                    st.experimental_rerun()
                else:
                    st.error('DOC number you entered is not valid. This is either because it is not in our database or it has not been catalogued')
            except Exception as e:
                st.error(f"An error occurred: {e}")
##-----------------------------------------------------------------------------------------------------------------##-----------------------------------------------------------------------------------------------------------------    
# PAGE 1
elif st.session_state.page == 1:
    doc_details = st.session_state.doc_details
    if doc_details is not None:
        st.success('Data Asset Registration record located')
        
        data_owner = doc_details.iloc[0]['PO_NAME']
        l1_process = doc_details.iloc[0]['PRIMARY_L1_BUSINESS_PROCESS']
        l2_process = doc_details.iloc[0]['L2_SOM_PROCESS']
        d_a_structure = doc_details.iloc[0]['DATA_ASSET_STRUCTURE']

        st.text_input('What is your L1 process/business domain?', value=l1_process, disabled=True)
        st.text_input('What is your L2 process/business domain?', value=l2_process, disabled=True)
        st.text_input('Does this Marketable Data Product contain Structured(Snowflake) or Unstructured(AWS) data?', value=d_a_structure, disabled=True)

        if d_a_structure == "Semi-structured":
            st.error('Currently we do not support the publishing of unstructured/semi-structured Data Assets. Please contact with the OneMesh team in case of any queries (onemesh@sanofi.com)')
    
        if d_a_structure == "Structured": 
            st.write("**How many Marketable Data Products do you wish to publish?**")
            st.session_state.num_mdps = st.number_input("*A Marketable Data Product (MDP) is a data product listed on OneMesh Marketplace that anyone can request access to. A marketable data product can represent one slice or the entire serve layer of the data asset. If you have divided your data asset into multiple data sets please select applicable count below:*", min_value=1, max_value=5, step=1)
        
            st.write("**Please provide the preferred name for the Marketable Data Product(s):**")
            mdp_name = st.markdown("*This name will be used to search the Marketable Data Product in OneMesh Marketplace. For example: M&S | Production Planning.*\n\n*More information [here](https://sanofi.atlassian.net/wiki/spaces/OneMesh/pages/64386044579/Data+Asset+Naming+and+Granularity+Standards+WIP#4.2.1-%E2%80%9CMarketable-Data-Product%E2%80%9D-Naming-Guideline)*")
            st.session_state.mdp_names = []
            for i in range(st.session_state.num_mdps):
                st.session_state.mdp_name = st.text_input(
                    f"Marketable Data Product {i+1} name",
                    #placeholder=f"Marketable Data Product {i+1} name",
                    key=f"mdp_{i}_name"
                )
                st.session_state.mdp_names.append(st.session_state.mdp_name)
        st.write(st.session_state.mdp_names)
    # Use two columns with slightly adjusted widths to bring buttons closer
    col11, col12 = st.columns([1.1, 9])
    
    with col11: 
        if st.button("Back"):
            st.session_state.page = 0
            st.experimental_rerun()
    
    with col12:
        if st.button("Next"):
            st.session_state.page = 2
            st.experimental_rerun()

    st.write("**Want to continue a previous session? Please enter the MDP ID given to you when you saved the session to prefill the fields**")
    mdp_id_fetch = st.text_input("MDP ID Provided:")
    if st.button("Continue Previous Session"):
        fetch_mdp_data(mdp_id_fetch,session)
        
##-----------------------------------------------------------------------------------------------------------------##-----------------------------------------------------------------------------------------------------------------
# PAGE 2
elif st.session_state.page == 2:
    num_mdps = st.session_state.num_mdps
    doc_details = st.session_state.doc_details
    st.title(f"Marketable Data Product Details")
    
    # Check if the data was fetched and is available in session state
    prefilled_data = st.session_state.get('mdp_data', {})
    ##----------------------------------------------------------------------------------------------------------------- Question 1
    st.session_state.data_owner_list = []
    st.write("**1- Who is the Data Owner of this Marketable Data Product? Please state the email:**")
    st.markdown("*Data owners are responsible for the data within their Marketable Data Product. They are also the approvers for the orders placed for their Marketable Data Products*")    
    for i in range(st.session_state.num_mdps):
        data_owner = prefilled_data.get('DATA_OWNERS', '') if prefilled_data else ''
        st.session_state.data_owner = st.text_input(
            f"{st.session_state.mdp_names[i]} Data Owner",
            value=data_owner,
            key=f"data_owner_{i}"
        )
        st.session_state.data_owner_list.append(st.session_state.data_owner)
    st.write(st.session_state.data_owner_list)

    ##----------------------------------------------------------------------------------------------------------------- Question 2
    st.session_state.description_list = []
    st.write("**2- Please provide a Description/purpose of the Marketable Data Product:**")
    st.markdown("*The description will be displayed for data consumers to understand more about the data product.*")
    for i in range(st.session_state.num_mdps):
        description = prefilled_data.get('DESCRIPTION', '') if prefilled_data else ''
        st.session_state.description = st.text_area(
            f"{st.session_state.mdp_names[i]} Description",
            value=description,
            key=f"mdp_{i}_description"
        )
        st.session_state.description_list.append(st.session_state.description)
    st.write(st.session_state.description_list)
    ##----------------------------------------------------------------------------------------------------------------- Question 3
    st.session_state.usage_criteria_list = []
    st.write("**3- Please add any Certified Usage criteria - where can this Marketable Data Product be used. The default applies for all Sanofi domains:**")
    st.markdown("*The contexts within which one can use this data, as certified by the data owner.*")
    for i in range(num_mdps):
        usage_criteria = prefilled_data.get('USAGE_CRITERIA', '').split(", ") if prefilled_data else []
        st.session_state.cert_usage_criteria = st.multiselect(
            f"{st.session_state.mdp_names[i]} Usage Criteria",
            st.session_state.usage_context,
            #default=usage_criteria,
            key=f"mdp_{i}_usage_criteria",
        )
        st.session_state.usage_criteria_list.append(st.session_state.cert_usage_criteria)
    st.write(st.session_state.usage_criteria_list)
    ##----------------------------------------------------------------------------------------------------------------- Question 4
    st.session_state.data_granularity_list = []    
    st.write("**4- Please provide the Level of Granularity of the data:**")
    st.markdown("*A short, specific level of detail at which the attributes and characteristics of the data in the data set are represented.*")
    for i in range(num_mdps):
        data_granularity = prefilled_data.get('GRANULARITY', '') if prefilled_data else ''
        st.session_state.data_granularity = st.text_input(
            f"{st.session_state.mdp_names[i]} Data Granularity",
            value=data_granularity,
            key=f"mdp_{i}_data_granularity"
        )
        st.session_state.data_granularity_list.append(st.session_state.data_granularity)
    st.write(st.session_state.data_granularity_list)

    ##----------------------------------------------------------------------------------------------------------------- Question 5
    st.session_state.data_refresh_frequency_list = []
    st.write("**5- What is the frequency of data refresh?**")
    st.markdown("*The frequency at which the data is refreshed in the underlying data platform.*")
    for i in range(num_mdps):
        refresh_frequency = prefilled_data.get('DATA_REFRESH_FREQUENCY', '') if prefilled_data else ''
        options = [f"Choose an option for {st.session_state.mdp_names[i]}", "Static", "Streaming", "Daily", "Weekly", "Fortnightly", "Monthly", "Bi-Monthly", "Quarterly", "Biannual", "Annual", "Biennial", "Triennial", "Other"]
        st.session_state.data_refresh_frequency = st.selectbox(
            f"{st.session_state.mdp_names[i]} Refresh Frequency",
            options,
            index=options.index(refresh_frequency) if refresh_frequency in options else 0,
            key=f"mdp_{i}_refresh_frequency"
        )
        if st.session_state.data_refresh_frequency == "Other":
            custom_data_freq = st.text_input(f"Please write down your own frequency of data refresh for {st.session_state.mdp_names[i]}")
            st.session_state.data_refresh_frequency = custom_data_freq
        st.session_state.data_refresh_frequency_list.append(st.session_state.data_refresh_frequency)
    st.write(st.session_state.data_refresh_frequency_list)

    ##----------------------------------------------------------------------------------------------------------------- Question 6
    st.session_state.mdp_tag_list = []
    st.write("**6- Please provide any “Tag” you want to apply to your Marketable Data Product:**")
    st.markdown("*This Tag used to search your Data Product in OneMesh Marketplace.*")
    for i in range(num_mdps):
        mdp_tags = prefilled_data.get('MDP_TAGS', '') if prefilled_data else ''
        st.session_state.mdp_tag = st.text_input(
            f"{st.session_state.mdp_names[i]} Tag(s)",
            value=mdp_tags,
            key=f"mdp_{i}_tag"
        )
        st.session_state.mdp_tag_list.append(st.session_state.mdp_tag)
    st.write(st.session_state.mdp_tag_list)

    ##----------------------------------------------------------------------------------------------------------------- Question 7
    st.session_state.storage_location_list = []
    st.write("**7- Please provide the data storage location link (Snowflake, AWS):**")
    st.markdown("*For example: Snowflake link*")
    for i in range(num_mdps):
        storage_location = prefilled_data.get('DATA_STORAGE_LOCATION_LINK', '') if prefilled_data else ''
        st.session_state.storage_location = st.text_input(
            f"{st.session_state.mdp_names[i]} Storage Location",
            value=storage_location,
            key=f"mdp_{i}_storage_location"
        )
        st.session_state.storage_location_list.append(st.session_state.storage_location)
    st.write(st.session_state.storage_location_list)

    ##----------------------------------------------------------------------------------------------------------------- Question 8
    st.session_state.delivery_formats_list = []
    st.write("**8- What are the supported delivery formats for this Marketable Data Product:**")
    st.markdown("*The delivery format represents the format in which the data is delivered*")
    for i in range(num_mdps):
        delivery_format = prefilled_data.get('DELIVERY_FORMAT', '') if prefilled_data else ''
        deliveryformatoptions = [f"Choose an option for {st.session_state.mdp_names[i]}", "Table", "View"]
        st.session_state.delivery_formats = st.selectbox(
            f"{st.session_state.mdp_names[i]} Delivery Format",
            deliveryformatoptions,
            index=deliveryformatoptions.index(delivery_format) if delivery_format in deliveryformatoptions else 0,
            key=f"mdp_{i}_delivery_format"
        )
        st.session_state.delivery_formats_list.append(st.session_state.delivery_formats)
    st.write(st.session_state.delivery_formats_list)

    ##----------------------------------------------------------------------------------------------------------------- Question 9
    st.session_state.ADoptions_list = []
    st.write("**9- Do you already have an AD group for your Marketable Data Product(s)?**")
    st.markdown("*The AD group will be used for providing Read-Only Access to the users.*")
    for i in range(num_mdps):
        ad_group = prefilled_data.get('AD_GROUP', '') if prefilled_data else ''
        ADoptions = [f"Choose an option for {st.session_state.mdp_names[i]}", "Yes", "No"]
        st.session_state.AD_group_confirmation = st.selectbox(
            f"AD Group Option",
            ADoptions,
            index=ADoptions.index("Yes") if ad_group else ADoptions.index("No"),
            key=f"mdp_{i}_AD_group_exists"
        )
        if st.session_state.AD_group_confirmation == "Yes":
            st.session_state.ad_group = st.text_input(
                f"Please provide the AD Group for {st.session_state.mdp_names[i]}",
                value=ad_group,
                key=f"mdp_{i}_ad_group"
            )
            st.session_state.AD_group_confirmation = st.session_state.ad_group 
        st.session_state.ADoptions_list.append(st.session_state.AD_group_confirmation)
    st.write(st.session_state.ADoptions_list)

    ##----------------------------------------------------------------------------------------------------------------- Question 10
    st.session_state.provider_account_list = []
    st.write("**10- Please provide the Snowflake Provider Account for your Marketable Data Product:**")
    st.markdown("*For example: EMEA_DF_FINANCE*")
    for i in range(num_mdps):
        provider_account = prefilled_data.get('PROVIDER_ACCOUNT', '') if prefilled_data else ''
        st.session_state.provider_account = st.text_input(
            f"Marketable Data Unit {i+1} Provider Account",
            value=provider_account,
            key=f"provider_account_{i}"
        )
        st.session_state.provider_account_list.append(st.session_state.provider_account)
    st.write(st.session_state.provider_account_list)

    ##----------------------------------------------------------------------------------------------------------------- Question 11
    st.session_state.additional_details_list = []
    st.write("**11- Are there any additional details you wish to add to your Marketable Data Product?**")
    st.markdown("*Free text field for any additional details you wish to add.*")
    for i in range(num_mdps):
        additional_detail = prefilled_data.get('ADDITIONAL_DETAIL', '') if prefilled_data else ''
        st.session_state.additional_details = st.text_area(
            f"{st.session_state.mdp_names[i]} Additional Details",
            value=additional_detail,
            key=f"mdp_{i}_additional_details"
        )
        st.session_state.additional_details_list.append(st.session_state.additional_details)
    st.write(st.session_state.additional_details_list)

    col21, col22, col20 = st.columns([1.1, 6, 2.5])

    save_mdp_success_message_placeholder = st.empty()
    
    with col20:
        if st.button("Save MDP Data"):
            insert_publishing_form_mdp_data()
            mdp_ids_formatted = "\n\n".join([f"**MDP {i+1}:** {mdp_id}" for i, mdp_id in enumerate(st.session_state.mdp_id)])
    
            # Add the final note to the message
            final_message = (
                f'**Your MDP Data has been saved successfully, and your MDP ID(s) are:**\n\n'
                f'{mdp_ids_formatted}\n\n'
                f'*Please keep these MDP ID\'s in hand as you\'ll need them to fetch the previous data in case you want to continue the form later on.*'
            )
    
            # Display the success message using the placeholder outside of the column layout
            save_mdp_success_message_placeholder.markdown(final_message)

            
    
    with col21: 
        if st.button("Back"):
            st.session_state.page = 1
            st.experimental_rerun()
    
    with col22:
        if st.button("Next"): 
            st.session_state.page = 3
            st.experimental_rerun()

#Page 3
elif st.session_state.page == 3:
    mdp_index = st.session_state.mdp_index
    
    # Check if mdp_names is not empty and mdp_index is within range
    if len(st.session_state.mdp_names) > 0 and mdp_index < len(st.session_state.mdp_names):
        mdp_name = st.session_state.mdp_names[mdp_index]
    else:
        st.error("MDP names list is empty or the index is out of range. Please check your input.")
        st.stop()  # Stop execution if the list is empty or index is out of range
    
    num_mdps = st.session_state.num_mdps
    
    st.header(f"Marketable Data Unit(s) Details for {mdp_name}")
    st.markdown("""
    
    *A Marketable Data Unit is a Data Asset that is within a Marketable Data Product. An MDP can contain multiple MDU's.* 
    
    """)
    st.write("**How Many Marketable Data Units do you wish to include in this Marketable Data Product?**")
    st.session_state.num_mdu = st.number_input("Please provide a number", min_value=1, max_value=10, step=1, key=f"num_mdu_{mdp_index}")

    st.session_state.dataset_links = []

    st.write("**1- Please provide the CDGC link to the dataset for your Marketable Data Unit(s):**")
    st.markdown("""
    *For example:*

    https://cdgc.dm-us.informaticacloud.com/asset/70b9cc13-b897-4731-81d0-419c30fee66e?type=Data+Set&name=VENDOR_MASTER_LFA1+on+DMT_PROCUREMENT_CONTRACTING


 
    """)
    for i in range(st.session_state.num_mdu):
        st.session_state.dataset_link = st.text_input(
            f"Marketable Data Unit {i+1} CDGC Link",
            #placeholder=f"Marketable Data Unit {i+1} CDGC Link",
            key=f"cdgc_link_{mdp_index}_{i}"
        )
        st.session_state.dataset_links.append(st.session_state.dataset_link)
    
    st.write(st.session_state.dataset_links)


    col31, col32 = st.columns([1.4,9])
    col33, col34 = st.columns([1.4,9])
    col35 = st.columns([1])
    
    if mdp_index < st.session_state.num_mdps - 1: 
        with col34:
            if st.button("Next Marketable Data Product"):
                st.session_state.mdp_index += 1
                st.experimental_rerun()

    success_message_placeholder = st.empty()
    
    with col33:
        if st.button("Finish"):
            st.session_state.publisher = st.session_state.data_owner_list
            doc_tool_number = st.session_state.doc_number
            insert_publishing_form_data()
            insert_publishing_form_mdu_data()
            # Display the success message in the placeholder outside of col33
            success_message_placeholder.success(f"Form submitted successfully! Your Form ID is {st.session_state.form_id}")
            # Reset session state for the relevant keys
            #st.session_state.mdp_index = 0
            st.session_state.mdp_data = {}  # Clear prefilled MDP data
            #st.session_state.num_mdps = 0
            #st.session_state.mdp_names = []
            st.session_state.data_owner_list = []
            st.session_state.description_list = []
            st.session_state.usage_criteria_list = []
            st.session_state.data_granularity_list = []
            st.session_state.data_refresh_frequency_list = []
            st.session_state.mdp_tag_list = []
            st.session_state.storage_location_list = []
            st.session_state.delivery_formats_list = []
            st.session_state.ADoptions_list = []
            st.session_state.provider_account_list = []
            st.session_state.additional_details_list = []
    
            # Rerun the app to clear the form fields

    ###################################################################################################333

    if mdp_index >= 1:  
        with col34:
            if st.button("Previous Marketable Data Product"):
                st.session_state.mdp_index -= 1
                st.experimental_rerun()

    with col32:
        if st.button("Return to Marketable Data Product Details"):
            st.session_state.page = 2
            st.experimental_rerun()

    with col31:
        if st.button("Restart"):
            # Reset session state for the relevant keys
            st.session_state.page = 0
            #st.session_state.mdp_index = 0
            st.session_state.mdp_data = {}  # Clear prefilled MDP data
            #st.session_state.num_mdps = 0
            #st.session_state.mdp_names = []
            st.session_state.data_owner_list = []
            st.session_state.description_list = []
            st.session_state.usage_criteria_list = []
            st.session_state.data_granularity_list = []
            st.session_state.data_refresh_frequency_list = []
            st.session_state.mdp_tag_list = []
            st.session_state.storage_location_list = []
            st.session_state.delivery_formats_list = []
            st.session_state.ADoptions_list = []
            st.session_state.provider_account_list = []
            st.session_state.additional_details_list = []

            # Rerun the app to clear the form fields
            st.experimental_rerun()
