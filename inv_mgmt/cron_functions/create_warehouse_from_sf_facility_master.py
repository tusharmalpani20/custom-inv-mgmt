import frappe
from frappe import _
import json
from frappe.utils import today

# Constants
PARENT_WAREHOUSE = "Darkstore - SFPL"
WAREHOUSE_TYPE = "Transit"
WAREHOUSE_CATEGORY = "Darkstore"

def get_branch_from_state(state):
    """
    Maps state to branch based on business rules.
    Returns None if state is not mapped to avoid creating warehouses for unmapped states.
    """
    state_to_branch_mapping = {
        "Telangana": "Hyderabad",
        "Karnataka": "Bengaluru"
    }
    return state_to_branch_mapping.get(state)

def get_branch_suffix(branch):
    """
    Returns the appropriate suffix for warehouse name based on branch
    """
    branch_suffix_mapping = {
        "Hyderabad": "HYD",
        "Bengaluru": "BLR"
    }
    return branch_suffix_mapping.get(branch)

def get_state_from_address(address_name):
    """
    Get state from address doctype
    """
    if not address_name:
        return None
        
    address = frappe.get_doc("Address", address_name)
    return address.state

def link_address_to_warehouse(warehouse_name, address_name):
    """
    Links an address to a warehouse using dynamic link
    """
    address = frappe.get_doc("Address", address_name)
    
    # Check if link already exists
    existing_link = next(
        (link for link in address.links if link.link_doctype == "Warehouse" and link.link_name == warehouse_name),
        None
    )
    
    if not existing_link:
        address.append("links", {
            "link_doctype": "Warehouse",
            "link_name": warehouse_name
        })
        address.save()

def create_error_log(reference_doctype=None, internal_reference=None, source_system=None, 
                     external_id=None, entity_type=None, error_category=None, 
                     error_severity="Medium", processing_stage=None, error_description=None, 
                     additional_detail=None):
    """
    Create an error log entry in SF Inventory Data Import Error Logs
    """
    try:
        error_log = frappe.new_doc("SF Inventory Data Import Error Logs")
        error_log.error_date = today()
        error_log.reference_doctype = reference_doctype
        error_log.internal_reference = internal_reference
        error_log.source_system = source_system
        error_log.external_id = external_id
        error_log.entity_type = entity_type
        error_log.error_category = error_category
        error_log.error_severity = error_severity
        error_log.processing_stage = processing_stage
        error_log.error_description = error_description
        if additional_detail:
            error_log.additional_detail = json.dumps(additional_detail)
        error_log.insert()
    except Exception as e:
        frappe.log_error(f"Failed to create error log: {str(e)}")

def create_warehouse_for_facility(facility):
    """
    Creates a warehouse for a given facility if conditions are met
    Returns the created warehouse doc or None
    """
    if not facility.shipping_address:
        msg = f"Skipping warehouse creation for facility {facility.name}: No shipping address"
        frappe.logger().debug(msg)
        create_error_log(
            reference_doctype="SF Facility Master",
            internal_reference=facility.name,
            source_system="Internal Processing",
            external_id=facility.name,
            entity_type="Facility",
            error_category="Missing Reference",
            error_severity="High",
            processing_stage="Warehouse Assignment",
            error_description=msg,
            additional_detail={"facility": facility.name}
        )
        return None
    
    state = get_state_from_address(facility.shipping_address)
    if not state:
        msg = f"Skipping warehouse creation for facility {facility.name}: No state in address"
        frappe.logger().debug(msg)
        create_error_log(
            reference_doctype="SF Facility Master",
            internal_reference=facility.name,
            source_system="Internal Processing",
            external_id=facility.name,
            entity_type="Facility",
            error_category="Invalid Address Data",
            error_severity="High",
            processing_stage="Warehouse Assignment",
            error_description=msg,
            additional_detail={"facility": facility.name, "address": facility.shipping_address}
        )
        return None
    
    branch = get_branch_from_state(state)
    if not branch:
        msg = f"Skipping warehouse creation for facility {facility.name}: State {state} not mapped to branch"
        frappe.logger().debug(msg)
        create_error_log(
            reference_doctype="SF Facility Master",
            internal_reference=facility.name,
            source_system="Internal Processing",
            external_id=facility.name,
            entity_type="Facility",
            error_category="Business Logic",
            error_severity="Medium",
            processing_stage="Warehouse Assignment",
            error_description=msg,
            additional_detail={"facility": facility.name, "state": state}
        )
        return None
    try:
        branch_suffix = get_branch_suffix(branch)
        warehouse_name = f"{facility.facility_name}-{branch_suffix}"
        # Get address details
        address = frappe.get_doc("Address", facility.shipping_address)
        warehouse = frappe.get_doc({
            "doctype": "Warehouse",
            "warehouse_name": warehouse_name,
            "company": frappe.defaults.get_defaults().get("company"),
            "parent_warehouse": PARENT_WAREHOUSE,
            "warehouse_type": WAREHOUSE_TYPE,
            "custom_warehouse_category": WAREHOUSE_CATEGORY,
            "custom_branch": branch,
            "custom_latitude": facility.latitude or 0.0,
            "custom_longitude": facility.longitude or 0.0,
            "address_line_1": address.address_line1,
            "address_line_2": address.address_line2,
            "city": address.city,
            "state": address.state,
            "pin": address.pincode,
            "phone_no": address.phone,
            "email_id": address.email_id
        })
        warehouse.insert()
        # Link address to warehouse
        link_address_to_warehouse(warehouse.name, facility.shipping_address)
        # Update facility with new warehouse
        frappe.db.set_value("SF Facility Master", facility.name, "warehouse", warehouse.name)
        return warehouse
    except Exception as e:
        msg = f"Error creating warehouse for facility {facility.name}: {str(e)}"
        frappe.logger().error(msg)
        create_error_log(
            reference_doctype="SF Facility Master",
            internal_reference=facility.name,
            source_system="Internal Processing",
            external_id=facility.name,
            entity_type="Warehouse",
            error_category="Warehouse Assignment",
            error_severity="Critical",
            processing_stage="Record Creation",
            error_description=msg,
            additional_detail={"facility": facility.name, "error": str(e)}
        )
        return None

def process_darkstore_facilities():
    """
    Main function to process all darkstore facilities without warehouses
    """
    facilities = frappe.get_all(
        "SF Facility Master",
        filters={
            "type": "Darkstore",
            "warehouse": ("is", "not set")
        },
        fields=["*"]
    )
    
    created_warehouses = []
    for facility in facilities:
        try:
            warehouse = create_warehouse_for_facility(frappe.get_doc("SF Facility Master", facility.name))
            if warehouse:
                created_warehouses.append(warehouse.name)
        except Exception as e:
            msg = f"Error creating warehouse for facility {facility.name}: {str(e)}"
            frappe.logger().error(msg)
            create_error_log(
                reference_doctype="SF Facility Master",
                internal_reference=facility.name,
                source_system="Internal Processing",
                external_id=facility.name,
                entity_type="Warehouse",
                error_category="Warehouse Assignment",
                error_severity="Critical",
                processing_stage="Record Creation",
                error_description=msg,
                additional_detail={"facility": facility.name, "error": str(e)}
            )
            continue
    return created_warehouses

@frappe.whitelist()
def create_missing_darkstore_warehouses():
    """
    API endpoint to trigger warehouse creation for darkstore facilities
    """
    if not frappe.has_permission("Warehouse", "create"):
        frappe.throw(_("Not permitted to create warehouses"))
        
    created = process_darkstore_facilities()
    return {
        "message": f"Created {len(created)} warehouses",
        "warehouses": created
    }

# this function is used to create warehouses for darkstore facilities that don't have warehouses
# bench execute "inv_mgmt.cron_functions.create_warehouse_from_sf_facility_master.create_missing_darkstore_warehouses"