import frappe
from frappe import _

def get_internal_customer():
    """
    Get internal customer for default company
    """
    default_company = frappe.defaults.get_defaults().get("company")
    internal_customer = frappe.get_value("Customer", {"is_internal_customer": 1, "represents_company": default_company}, "name")
    
    if not internal_customer:
        frappe.throw(_("No internal customer found for company {0}").format(default_company))
    
    return internal_customer

def link_address_to_customer(address_name, customer):
    """
    Links an address to a customer using dynamic link if not already linked
    """
    # Validate address exists
    if not frappe.db.exists("Address", address_name):
        print(f"Address {address_name} does not exist")
        return False
        
    address = frappe.get_doc("Address", address_name)
    
    # Check if link already exists
    existing_link = next(
        (link for link in address.links if link.link_doctype == "Customer" and link.link_name == customer),
        None
    )
    
    if not existing_link:
        address.append("links", {
            "link_doctype": "Customer",
            "link_name": customer
        })
        address.save()
        print(f"Linked address {address_name} to customer {customer}")
        return True
    else:
        print(f"Address {address_name} already linked to customer {customer}")
        return True

def process_darkstore_addresses():
    """
    Process all darkstore facilities whose addresses are not linked to internal customer
    """
    # Get internal customer
    internal_customer = get_internal_customer()
    
    # Get all darkstore facilities where address is not linked
    facilities = frappe.get_all(
        "SF Facility Master",
        filters={
            "type": "Darkstore",
            "is_address_linked_to_internal_customer": 0,
            "shipping_address": ("is", "set"),
            "warehouse": ("is", "set")  # Only consider facilities with warehouse
        },
        fields=["name", "facility_name", "shipping_address", "warehouse"]
    )
    
    if not facilities:
        print("No eligible darkstore facilities found with unlinked addresses")
        return
    
    print(f"Found {len(facilities)} darkstore facilities with unlinked addresses")
    
    for facility in facilities:
        try:
            # Verify both warehouse and address exist
            if not frappe.db.exists("Warehouse", facility.warehouse):
                print(f"Skipping facility {facility.facility_name}: Warehouse {facility.warehouse} does not exist")
                continue
                
            if not frappe.db.exists("Address", facility.shipping_address):
                print(f"Skipping facility {facility.facility_name}: Address {facility.shipping_address} does not exist")
                continue
            
            # Link address to internal customer
            if link_address_to_customer(facility.shipping_address, internal_customer):
                # Mark facility as linked only if linking was successful
                frappe.db.set_value(
                    "SF Facility Master", 
                    facility.name, 
                    "is_address_linked_to_internal_customer", 
                    1
                )
                print(f"Marked facility {facility.facility_name} as linked")
            
        except Exception as e:
            print(f"Error processing facility {facility.facility_name}: {str(e)}")
            continue

@frappe.whitelist()
def link_darkstore_addresses_to_internal_customer():
    """
    API endpoint to trigger linking of darkstore addresses to internal customer
    """
    if not frappe.has_permission("Customer", "write"):
        frappe.throw(_("Not permitted to modify customer"))
    
    process_darkstore_addresses()
    return {
        "message": "Completed processing darkstore addresses"
    }

# this function is used to link the darkstore addresses to the internal customer; 
# We generally run this after the darkstore facilities are created and once we have created darkstore warehouses
# bench execute "inv_mgmt.cron_functions.add_darkstore_address_to_internal_customer.link_darkstore_addresses_to_internal_customer"
