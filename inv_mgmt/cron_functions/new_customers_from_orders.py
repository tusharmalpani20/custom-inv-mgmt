import frappe
from frappe.utils import today, cstr
from typing import Dict, Any, List, Optional
import json
import re

# Import the existing address creation functions to avoid duplication
try:
    from inv_mgmt.cron_functions.create_address_from_lat_long import (
        get_address_from_nominatim,
        is_valid_coordinates,
        get_address_line1,
        get_address_line2,
        get_city,
        get_state,
        get_country,
        get_pincode,
        NOMINATIM_API_DELAY_SECONDS
    )
    import time
except ImportError:
    # Fallback if import fails
    import requests
    import time
    NOMINATIM_API_DELAY_SECONDS = 1


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


def create_new_customers_from_orders():
    """
    Create SF Inventory External ID Mapping records for new customers from B2B orders.
    
    Process:
    1. Get B2B orders from SF Order Master for today's order_date
    2. Extract unique customer information from these orders
    3. Create SF Inventory External ID Mapping records if they don't already exist
    4. Store customer details in additional_details JSON field
    
    Returns:
        Dictionary with success status, message, and processing results
    """
    try:
        print("Starting new customers from orders process...")
        
        # Get B2B orders for today
        b2b_orders = get_b2b_orders_for_today()
        
        if not b2b_orders:
            print("No B2B orders found for today")
            return {
                "success": True,
                "message": "No B2B orders found for today",
                "processed": 0
            }
        
        print(f"Found {len(b2b_orders)} B2B orders for today")
        
        # Extract unique customers from orders
        unique_customers = extract_unique_customers(b2b_orders)
        
        if not unique_customers:
            print("No unique customers found in B2B orders")
            return {
                "success": True,
                "message": "No unique customers found in B2B orders",
                "processed": 0
            }
        
        print(f"Found {len(unique_customers)} unique customers")
        
        # Start transaction for all mapping creation
        frappe.db.begin()
        
        success_count = 0
        error_count = 0
        errors = []
        
        for customer_data in unique_customers:
            try:
                print(f"Processing customer: {customer_data.get('customer_id')} - {customer_data.get('customer_name')}")
                
                # Check if mapping already exists
                if not mapping_exists(customer_data.get('customer_id')):
                    # Create new mapping
                    create_customer_mapping(customer_data)
                    success_count += 1
                    print(f"Successfully created mapping for customer: {customer_data.get('customer_id')}")
                else:
                    print(f"Mapping already exists for customer: {customer_data.get('customer_id')}")
                    
            except Exception as e:
                error_count += 1
                error_detail = {
                    "customer_id": customer_data.get("customer_id"),
                    "customer_name": customer_data.get("customer_name"),
                    "error": str(e)
                }
                errors.append(error_detail)
                
                print(f"Error processing customer {customer_data.get('customer_id')}: {str(e)}")
                create_error_log(
                    reference_doctype="SF Inventory External ID Mapping",
                    internal_reference=None,
                    source_system="SF Order API",
                    external_id=customer_data.get("customer_id"),
                    entity_type="Customer",
                    error_category="Customer Creation",
                    error_severity="High",
                    processing_stage="Record Creation",
                    error_description=f"Error creating customer mapping: {str(e)}",
                    additional_detail={"customer_data": customer_data, "error": str(e)}
                )
        
        # Commit transaction
        frappe.db.commit()
        
        print(f"New customers from orders process completed. Success: {success_count}, Errors: {error_count}")
        
        return {
            "success": True,
            "message": f"New customers from orders process completed. Success: {success_count}, Errors: {error_count}",
            "processed": success_count,
            "errors": errors if errors else None
        }
        
    except Exception as e:
        frappe.db.rollback()
        print(f"Critical error during new customers from orders process: {str(e)}")
        frappe.log_error(
            title="New Customers from Orders - Critical Error",
            message=f"Error: {str(e)}\nTraceback: {frappe.get_traceback()}"
        )
        return {
            "success": False,
            "message": f"Critical error during new customers from orders process: {str(e)}"
        }


def create_customers_from_external_mappings():
    """
    Create actual Customer records from SF Inventory External ID Mapping records.
    
    Process:
    1. Get SF Inventory External ID Mapping records with entity_type=Customer, 
       source_system=SF Order API, and no internal_reference
    2. For each mapping, create a Customer record
    3. Update the mapping with the created Customer reference
    
    Returns:
        Dictionary with success status, message, and processing results
    """
    try:
        print("Starting customer creation from external mappings process...")
        
        # Get external mappings that need Customer records
        external_mappings = get_external_mappings_needing_customers()
        
        if not external_mappings:
            print("No external mappings found that need Customer creation")
            return {
                "success": True,
                "message": "No external mappings found that need Customer creation",
                "processed": 0
            }
        
        print(f"Found {len(external_mappings)} external mappings that need Customer creation")
        
        # Start transaction for all customer creation
        frappe.db.begin()
        
        success_count = 0
        error_count = 0
        errors = []
        
        for mapping_data in external_mappings:
            try:
                print(f"Processing mapping: {mapping_data.get('name')} - {mapping_data.get('external_name')}")
                
                # Create Customer record
                customer_name = create_customer_from_mapping(mapping_data)
                
                if customer_name:
                    # Update mapping with Customer reference
                    update_mapping_with_customer_reference(mapping_data.get('name'), customer_name)
                    success_count += 1
                    print(f"Successfully created customer: {customer_name}")
                else:
                    error_count += 1
                    errors.append({
                        "mapping_name": mapping_data.get("name"),
                        "external_id": mapping_data.get("external_id"),
                        "error": "Failed to create customer record"
                    })
                    create_error_log(
                        reference_doctype="Customer",
                        internal_reference=None,
                        source_system="SF Order API",
                        external_id=mapping_data.get("external_id"),
                        entity_type="Customer",
                        error_category="Customer Creation",
                        error_severity="High",
                        processing_stage="Record Creation",
                        error_description="Failed to create customer record from mapping.",
                        additional_detail={"mapping_data": mapping_data}
                    )
                    
            except Exception as e:
                error_count += 1
                error_detail = {
                    "mapping_name": mapping_data.get("name"),
                    "external_id": mapping_data.get("external_id"),
                    "external_name": mapping_data.get("external_name"),
                    "error": str(e)
                }
                errors.append(error_detail)
                
                print(f"Error processing mapping {mapping_data.get('name')}: {str(e)}")
                create_error_log(
                    reference_doctype="Customer",
                    internal_reference=None,
                    source_system="SF Order API",
                    external_id=mapping_data.get("external_id"),
                    entity_type="Customer",
                    error_category="Customer Creation",
                    error_severity="High",
                    processing_stage="Record Creation",
                    error_description=f"Error creating customer from mapping: {str(e)}",
                    additional_detail={"mapping_data": mapping_data, "error": str(e)}
                )
        
        # Commit transaction
        frappe.db.commit()
        
        print(f"Customer creation from external mappings completed. Success: {success_count}, Errors: {error_count}")
        
        return {
            "success": True,
            "message": f"Customer creation from external mappings completed. Success: {success_count}, Errors: {error_count}",
            "processed": success_count,
            "errors": errors if errors else None
        }
        
    except Exception as e:
        frappe.db.rollback()
        print(f"Critical error during customer creation from external mappings: {str(e)}")
        frappe.log_error(
            title="Customer Creation from External Mappings - Critical Error",
            message=f"Error: {str(e)}\nTraceback: {frappe.get_traceback()}"
        )
        return {
            "success": False,
            "message": f"Critical error during customer creation from external mappings: {str(e)}"
        }


def get_external_mappings_needing_customers() -> List[Dict[str, Any]]:
    """
    Get SF Inventory External ID Mapping records that need Customer records created.
    
    Returns:
        List of mapping dictionaries
    """
    try:
        mappings = frappe.get_all(
            "SF Inventory External ID Mapping",
            filters={
                "entity_type": "Customer",
                "source_system": "SF Order API",
                "internal_reference": ["is", "not set"]
            },
            fields=[
                "name", "external_id", "external_name", "additional_details"
            ]
        )
        
        return mappings
        
    except Exception as e:
        frappe.log_error(
            title="Get External Mappings Needing Customers - Error",
            message=f"Error: {str(e)}\nTraceback: {frappe.get_traceback()}"
        )
        return []


def create_customer_from_mapping(mapping_data: Dict[str, Any]) -> Optional[str]:
    """
    Create a Customer record from external mapping data.
    
    Args:
        mapping_data: External mapping dictionary
    
    Returns:
        Name of the created Customer record or None if failed
    """
    try:
        # Parse additional details
        additional_details = {}
        if mapping_data.get("additional_details"):
            try:
                additional_details = json.loads(mapping_data["additional_details"])
            except json.JSONDecodeError:
                print(f"Warning: Could not parse additional_details for mapping {mapping_data.get('name')}")
        
        # Get customer category and create/get customer group
        customer_category = additional_details.get("customer_category", "General")
        customer_group = get_or_create_customer_group(customer_category)
        
        # Get GSTIN and determine GST category
        gstin = additional_details.get("customer_gstin", "")
        gst_category = determine_gst_category(gstin)
        
        # Create Customer document
        customer = frappe.new_doc("Customer")
        customer.customer_name = mapping_data.get("external_name") or f"Customer {mapping_data.get('external_id')}"
        customer.customer_type = "Company"
        customer.customer_group = customer_group
        
        # Set GSTIN if valid
        if gstin and is_valid_gstin(gstin):
            customer.gstin = gstin
            customer.tax_id = gstin
            customer.gst_category = gst_category
        else:
            customer.gst_category = "Unregistered"
        
        # Set default territory (you may want to customize this)
        customer.territory = get_default_territory()
        
        # Insert the customer record
        customer.insert()
        
        print(f"Created Customer: {customer.name} for external ID: {mapping_data.get('external_id')}")
        return customer.name
        
    except Exception as e:
        print(f"Error creating customer from mapping: {str(e)}")
        frappe.log_error(
            title="Create Customer from Mapping - Error",
            message=f"Error: {str(e)}\nMapping Data: {json.dumps(mapping_data, indent=2)}\nTraceback: {frappe.get_traceback()}"
        )
        return None


def get_or_create_customer_group(customer_category: str) -> str:
    """
    Get existing customer group or create new one based on customer category.
    
    Args:
        customer_category: Category name from additional details
    
    Returns:
        Customer group name
    """
    try:
        # Check if customer group already exists
        existing_group = frappe.db.get_value("Customer Group", customer_category, "name")
        
        if existing_group:
            print(f"Using existing customer group: {existing_group}")
            return existing_group
        
        # Create new customer group
        customer_group = frappe.new_doc("Customer Group")
        customer_group.customer_group_name = customer_category
        customer_group.parent_customer_group = get_default_parent_customer_group()
        customer_group.is_group = 0  # Leaf node
        
        customer_group.insert()
        
        print(f"Created new customer group: {customer_group.name}")
        return customer_group.name
        
    except Exception as e:
        print(f"Error creating customer group {customer_category}: {str(e)}")
        frappe.log_error(
            title="Create Customer Group - Error",
            message=f"Error: {str(e)}\nCustomer Category: {customer_category}\nTraceback: {frappe.get_traceback()}"
        )
        # Return default customer group as fallback
        return get_default_customer_group()


def get_default_parent_customer_group() -> str:
    """Get default parent customer group."""
    # Try to get "All Customer Groups" first
    parent_group = frappe.db.get_value("Customer Group", {"is_group": 1}, "name")
    if parent_group:
        return parent_group
    
    # Fallback to creating one if needed
    return "All Customer Groups"


def get_default_customer_group() -> str:
    """Get default customer group as fallback."""
    default_group = frappe.db.get_value("Customer Group", {"is_group": 0}, "name")
    if default_group:
        return default_group
    
    return "General"


def get_default_territory() -> str:
    """Get default territory for customers."""
    # Try to get a default territory
    territory = frappe.db.get_value("Territory", {"is_group": 0}, "name")
    if territory:
        return territory
    
    # Try "All Territories" as parent
    parent_territory = frappe.db.get_value("Territory", {"is_group": 1}, "name")
    if parent_territory:
        return parent_territory
    
    return "All Territories"


def is_valid_gstin(gstin: str) -> bool:
    """
    Validate GSTIN format.
    
    Args:
        gstin: GSTIN string to validate
    
    Returns:
        True if valid GSTIN format, False otherwise
    """
    if not gstin:
        return False
    
    # GSTIN format: 15 characters - 2 state code + 10 PAN + 1 entity code + 1 Z + 1 check digit
    gstin_pattern = r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}[Z]{1}[0-9A-Z]{1}$'
    
    return bool(re.match(gstin_pattern, gstin.upper()))


def determine_gst_category(gstin: str) -> str:
    """
    Determine GST category based on GSTIN.
    
    Args:
        gstin: GSTIN string
    
    Returns:
        GST category string
    """
    if not gstin or not is_valid_gstin(gstin):
        return "Unregistered"
    
    # For now, we'll use simple logic
    # You can enhance this based on more specific business rules
    
    # Check entity code (4th character from right, or 12th character from left)
    if len(gstin) >= 12:
        entity_code = gstin[11].upper()
        
        # Common entity codes:
        # 1-9: Regular business
        # A-Z: Other types
        if entity_code in ['1', '2', '3', '4', '5', '6', '7', '8', '9']:
            return "Registered Regular"
        elif entity_code in ['A', 'B', 'C', 'F', 'G', 'H', 'J', 'K', 'L', 'T']:
            return "Registered Regular"
        else:
            return "Registered Regular"
    
    return "Registered Regular"


def update_mapping_with_customer_reference(mapping_name: str, customer_name: str) -> bool:
    """
    Update SF Inventory External ID Mapping with Customer reference.
    
    Args:
        mapping_name: Name of the mapping record
        customer_name: Name of the created Customer record
    
    Returns:
        True if successful, False otherwise
    """
    try:
        mapping_doc = frappe.get_doc("SF Inventory External ID Mapping", mapping_name)
        mapping_doc.reference_doctype = "Customer"
        mapping_doc.internal_reference = customer_name
        mapping_doc.save()
        
        print(f"Updated mapping {mapping_name} with customer reference {customer_name}")
        return True
        
    except Exception as e:
        print(f"Error updating mapping {mapping_name} with customer reference: {str(e)}")
        frappe.log_error(
            title=f"Update Mapping with Customer Reference - Error",
            message=f"Error: {str(e)}\nMapping: {mapping_name}\nCustomer: {customer_name}\nTraceback: {frappe.get_traceback()}"
        )
        return False


def get_b2b_orders_for_today() -> List[Dict[str, Any]]:
    """
    Get B2B orders from SF Order Master for today's order_date.
    
    Returns:
        List of order dictionaries with customer details
    """
    try:
        orders = frappe.get_all(
            "SF Order Master",
            filters={
                "order_type": "B2B",
                # "order_date": today()
                "order_date": "2025-07-15"
            },
            fields=[
                "name", "order_id", "customer_id", "customer_name", 
                "customer_gstin", "customer_billing_name", "customer_category",
                "delivery_latitude", "delivery_longitude", "delivery_address"
            ]
        )
        
        return orders
        
    except Exception as e:
        frappe.log_error(
            title="Get B2B Orders for Today - Error",
            message=f"Error: {str(e)}\nTraceback: {frappe.get_traceback()}"
        )
        return []


def extract_unique_customers(orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extract unique customers from the list of orders.
    
    Args:
        orders: List of order dictionaries
    
    Returns:
        List of unique customer dictionaries
    """
    try:
        unique_customers_dict = {}
        
        for order in orders:
            customer_id = order.get("customer_id")
            
            if customer_id and customer_id not in unique_customers_dict:
                customer_data = {
                    "customer_id": customer_id,
                    "customer_name": order.get("customer_name"),
                    "customer_gstin": order.get("customer_gstin"),
                    "customer_billing_name": order.get("customer_billing_name"),
                    "customer_category": order.get("customer_category"),
                    "delivery_latitude": order.get("delivery_latitude"),
                    "delivery_longitude": order.get("delivery_longitude"),
                    "delivery_address": order.get("delivery_address")
                }
                
                unique_customers_dict[customer_id] = customer_data
        
        return list(unique_customers_dict.values())
        
    except Exception as e:
        frappe.log_error(
            title="Extract Unique Customers - Error",
            message=f"Error: {str(e)}\nTraceback: {frappe.get_traceback()}"
        )
        return []


def mapping_exists(customer_id: str) -> bool:
    """
    Check if SF Inventory External ID Mapping already exists for the customer.
    
    Args:
        customer_id: External customer ID
    
    Returns:
        True if mapping exists, False otherwise
    """
    try:
        existing_mapping = frappe.get_all(
            "SF Inventory External ID Mapping",
            filters={
                "external_id": customer_id,
                "entity_type": "Customer",
                "source_system": "SF Order API"
            },
            fields=["name"]
        )
        
        return len(existing_mapping) > 0
        
    except Exception as e:
        frappe.log_error(
            title="Mapping Exists Check - Error",
            message=f"Error: {str(e)}\nCustomer ID: {customer_id}\nTraceback: {frappe.get_traceback()}"
        )
        return False


def create_customer_mapping(customer_data: Dict[str, Any]) -> str:
    """
    Create SF Inventory External ID Mapping record for a customer.
    
    Args:
        customer_data: Customer information dictionary
    
    Returns:
        Name of the created mapping record
    """
    try:
        # Prepare additional details JSON
        additional_details = {
            "customer_gstin": customer_data.get("customer_gstin"),
            "customer_billing_name": customer_data.get("customer_billing_name"),
            "customer_category": customer_data.get("customer_category"),
            "delivery_latitude": customer_data.get("delivery_latitude"),
            "delivery_longitude": customer_data.get("delivery_longitude"),
            "delivery_address": customer_data.get("delivery_address")
        }
        
        # Remove None values from additional_details
        additional_details = {k: v for k, v in additional_details.items() if v is not None}
        
        # Create SF Inventory External ID Mapping document
        mapping = frappe.new_doc("SF Inventory External ID Mapping")
        mapping.external_id = customer_data.get("customer_id")
        mapping.external_name = customer_data.get("customer_name") or f"Customer {customer_data.get('customer_id')}"
        mapping.entity_type = "Customer"
        mapping.source_system = "SF Order API"
        mapping.additional_details = json.dumps(additional_details) if additional_details else None
        
        # Note: reference_doctype and internal_reference are not set initially
        # They can be set later when actual Customer records are created
        
        mapping.insert()
        
        print(f"Created SF Inventory External ID Mapping: {mapping.name}")
        return mapping.name
        
    except Exception as e:
        print(f"Error creating customer mapping: {str(e)}")
        frappe.log_error(
            title="Create Customer Mapping - Error",
            message=f"Error: {str(e)}\nCustomer Data: {json.dumps(customer_data, indent=2)}\nTraceback: {frappe.get_traceback()}"
        )
        raise


@frappe.whitelist()
def run_new_customers_from_orders():
    """
    API endpoint to manually trigger new customers from orders process.
    """
    if not frappe.has_permission("SF Inventory External ID Mapping", "create"):
        frappe.throw("Not permitted to create SF Inventory External ID Mapping records")
    
    result = create_new_customers_from_orders()
    return result


@frappe.whitelist()
def run_create_customers_from_external_mappings():
    """
    API endpoint to manually trigger customer creation from inventory external mappings.
    """
    if not frappe.has_permission("Customer", "create"):
        frappe.throw("Not permitted to create Customer records")
    
    result = create_customers_from_external_mappings()
    return result


def create_addresses_for_b2b_customers():
    """
    Create Address records for B2B customers from SF Inventory External ID Mapping
    that have latitude/longitude coordinates but no primary address set.
    
    Process:
    1. Get SF Inventory External ID Mapping records for Customer entity with SF Order API source
    2. For each mapping with linked customer, check if customer needs address
    3. Extract lat/long from additional_details and call Nominatim API
    4. Create Address record and set as customer's primary and shipping address
    5. Respects 1 second rate limit between API calls
    """
    try:
        print("Starting address creation for B2B customers process...")
        
        # Get customer mappings that need address creation
        customer_mappings = get_customer_mappings_needing_addresses()
        
        if not customer_mappings:
            print("No customer mappings found that need address creation")
            return {
                "success": True,
                "message": "No customer mappings found that need address creation",
                "processed": 0
            }
        
        print(f"Found {len(customer_mappings)} customer mappings that need address creation")
        
        # Start transaction for all address creation
        frappe.db.begin()
        
        success_count = 0
        error_count = 0
        errors = []
        
        for mapping in customer_mappings:
            try:
                print(f"Processing customer mapping: {mapping.get('name')} - {mapping.get('external_name')}")
                
                # Parse additional details to get coordinates
                coordinates = extract_coordinates_from_mapping(mapping)
                
                if not coordinates:
                    print(f"No valid coordinates found for mapping {mapping.get('name')}")
                    create_error_log(
                        reference_doctype="Customer",
                        internal_reference=mapping.get("internal_reference"),
                        source_system="SF Order API",
                        external_id=mapping.get("external_id"),
                        entity_type="Customer",
                        error_category="Invalid Coordinates",
                        error_severity="Medium",
                        processing_stage="Address Generation",
                        error_description="No valid coordinates found for address creation.",
                        additional_detail={"mapping": mapping}
                    )
                    continue
                
                # Get address data from Nominatim API (reusing existing function)
                address_data = get_address_from_nominatim(coordinates['latitude'], coordinates['longitude'])
                
                if address_data:
                    # Create Address record
                    address_name = create_address_record_for_customer(mapping, address_data, coordinates)
                    
                    if address_name:
                        # Update Customer with primary and shipping address
                        update_customer_addresses(mapping.get('internal_reference'), address_name)
                        success_count += 1
                        print(f"Successfully created address for customer: {mapping.get('internal_reference')}")
                    else:
                        error_count += 1
                        errors.append({
                            "mapping": mapping.get("name"),
                            "customer": mapping.get("internal_reference"),
                            "error": "Failed to create address record"
                        })
                        create_error_log(
                            reference_doctype="Customer",
                            internal_reference=mapping.get("internal_reference"),
                            source_system="SF Order API",
                            external_id=mapping.get("external_id"),
                            entity_type="Customer",
                            error_category="Address Generation",
                            error_severity="High",
                            processing_stage="Record Creation",
                            error_description="Failed to create address record for customer.",
                            additional_detail={"mapping": mapping, "address_data": address_data}
                        )
                else:
                    error_count += 1
                    errors.append({
                        "mapping": mapping.get("name"),
                        "customer": mapping.get("internal_reference"),
                        "error": "Failed to get address data from Nominatim API"
                    })
                    create_error_log(
                        reference_doctype="Customer",
                        internal_reference=mapping.get("internal_reference"),
                        source_system="SF Order API",
                        external_id=mapping.get("external_id"),
                        entity_type="Customer",
                        error_category="Address API Error",
                        error_severity="High",
                        processing_stage="Address API Call",
                        error_description="Failed to get address data from Nominatim API.",
                        additional_detail={"mapping": mapping, "coordinates": coordinates}
                    )
                
                # Rate limiting - wait 1 second between API calls
                if len(customer_mappings) > 1:  # Only wait if there are more mappings to process
                    print(f"Waiting {NOMINATIM_API_DELAY_SECONDS} seconds before next API call...")
                    time.sleep(NOMINATIM_API_DELAY_SECONDS)
                
            except Exception as e:
                error_count += 1
                error_detail = {
                    "mapping": mapping.get("name"),
                    "customer": mapping.get("internal_reference"),
                    "error": str(e)
                }
                errors.append(error_detail)
                print(f"Error processing customer mapping {mapping.get('name')}: {str(e)}")
                create_error_log(
                    reference_doctype="Customer",
                    internal_reference=mapping.get("internal_reference"),
                    source_system="SF Order API",
                    external_id=mapping.get("external_id"),
                    entity_type="Customer",
                    error_category="Address Generation",
                    error_severity="Critical",
                    processing_stage="Record Creation",
                    error_description=f"Error creating address for customer: {str(e)}",
                    additional_detail={"mapping": mapping, "error": str(e)}
                )
        
        # Commit transaction
        frappe.db.commit()
        
        print(f"Customer address creation completed. Success: {success_count}, Errors: {error_count}")
        
        return {
            "success": True,
            "message": f"Customer address creation completed. Success: {success_count}, Errors: {error_count}",
            "processed": success_count,
            "errors": errors if errors else None
        }
        
    except Exception as e:
        frappe.db.rollback()
        print(f"Critical error during customer address creation: {str(e)}")
        frappe.log_error(
            title="Customer Address Creation from Lat/Long - Critical Error",
            message=f"Error: {str(e)}\nTraceback: {frappe.get_traceback()}"
        )
        return {
            "success": False,
            "message": f"Critical error during customer address creation: {str(e)}"
        }


def get_customer_mappings_needing_addresses() -> List[Dict[str, Any]]:
    """
    Get SF Inventory External ID Mapping records for customers that need address creation.
    
    Returns:
        List of mapping dictionaries with customer details
    """
    try:
        # Get all customer mappings with linked customers
        mappings = frappe.get_all(
            "SF Inventory External ID Mapping",
            filters={
                "entity_type": "Customer",
                "source_system": "SF Order API",
                "internal_reference": ["is", "set"]
            },
            fields=[
                "name", "external_id", "external_name", "internal_reference", "additional_details"
            ]
        )
        
        # Filter mappings where linked customer doesn't have primary address
        mappings_needing_address = []
        
        for mapping in mappings:
            customer_name = mapping.get("internal_reference")
            
            if customer_name and frappe.db.exists("Customer", customer_name):
                # Check if customer has primary address
                customer_primary_address = frappe.db.get_value("Customer", customer_name, "customer_primary_address")
                
                if not customer_primary_address:
                    # Check if mapping has coordinates in additional_details
                    coordinates = extract_coordinates_from_mapping(mapping)
                    if coordinates:
                        mappings_needing_address.append(mapping)
                        print(f"Customer {customer_name} needs address creation")
                    else:
                        print(f"Customer {customer_name} has no coordinates in mapping")
                else:
                    print(f"Customer {customer_name} already has primary address")
        
        return mappings_needing_address
        
    except Exception as e:
        frappe.log_error(
            title="Get Customer Mappings Needing Addresses - Error",
            message=f"Error: {str(e)}\nTraceback: {frappe.get_traceback()}"
        )
        return []


def extract_coordinates_from_mapping(mapping: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """
    Extract latitude and longitude coordinates from mapping's additional_details.
    
    Args:
        mapping: Mapping dictionary with additional_details
    
    Returns:
        Dictionary with latitude and longitude or None if not found
    """
    try:
        additional_details = mapping.get("additional_details")
        if not additional_details:
            return None
        
        if isinstance(additional_details, str):
            try:
                additional_details = json.loads(additional_details)
            except json.JSONDecodeError:
                print(f"Warning: Could not parse additional_details for mapping {mapping.get('name')}")
                return None
        
        latitude = additional_details.get("delivery_latitude")
        longitude = additional_details.get("delivery_longitude")
        
        if latitude and longitude:
            # Validate coordinates using the existing function
            try:
                lat = float(latitude)
                lon = float(longitude)
                
                if is_valid_coordinates(lat, lon):
                    return {
                        "latitude": str(latitude),
                        "longitude": str(longitude)
                    }
                else:
                    print(f"Invalid coordinates for mapping {mapping.get('name')}: {lat}, {lon}")
            except (ValueError, TypeError):
                print(f"Non-numeric coordinates for mapping {mapping.get('name')}: {latitude}, {longitude}")
                create_error_log(
                    reference_doctype="Customer",
                    internal_reference=mapping.get("internal_reference"),
                    source_system="SF Order API",
                    external_id=mapping.get("external_id"),
                    entity_type="Customer",
                    error_category="Invalid Coordinates",
                    error_severity="Critical",
                    processing_stage="Address Generation",
                    error_description=f"Non-numeric coordinates for mapping {mapping.get('name')}: {latitude}, {longitude}",
                    additional_detail={"mapping": mapping, "latitude": latitude, "longitude": longitude}
                )
                return None
        
        return None
        
    except Exception as e:
        print(f"Error extracting coordinates from mapping: {str(e)}")
        return None


def create_address_record_for_customer(mapping: Dict[str, Any], address_data: Dict[str, Any], coordinates: Dict[str, str]) -> Optional[str]:
    """
    Create Address record from Nominatim API data and link it to the Customer.
    Uses the existing address parsing functions from create_address_from_lat_long.py
    
    Args:
        mapping: SF Inventory External ID Mapping data
        address_data: Address data from Nominatim API
        coordinates: Latitude and longitude coordinates
    
    Returns:
        Address record name if successful, None otherwise
    """
    try:
        customer_name = mapping.get("internal_reference")
        customer_doc = frappe.get_doc("Customer", customer_name)
        
        # Extract address components from Nominatim response using existing functions
        address_components = address_data.get("address", {})
        
        # Map Nominatim fields to Address fields (reusing existing functions)
        address_line1 = get_address_line1(address_components)
        address_line2 = get_address_line2(address_components)
        city = get_city(address_components)
        state = get_state(address_components)
        country = get_country(address_components)
        pincode = get_pincode(address_components)
        
        # Log what we found for debugging
        print(f"Address components for customer {customer_name}:")
        print(f"  Address Line 1: {address_line1}")
        print(f"  Address Line 2: {address_line2}")
        print(f"  City: {city}")
        print(f"  State: {state}")
        print(f"  Country: {country}")
        print(f"  Pincode: {pincode}")
        
        # Validate required fields
        if not address_line1 or address_line1 == "Location not specified":
            print(f"No valid address line 1 found for customer {customer_name}")
            print(f"Available address components: {list(address_components.keys())}")
            return None
        
        if not city or city == "Unknown City":
            print(f"No valid city found for customer {customer_name}")
            print(f"Available address components: {list(address_components.keys())}")
            return None
        
        if not country:
            print(f"No valid country found for customer {customer_name}")
            print(f"Available address components: {list(address_components.keys())}")
            return None
        
        # Create Address document
        address_doc = frappe.new_doc("Address")
        
        # Set address fields
        address_doc.address_title = customer_doc.customer_name  # Use customer name as address title
        address_doc.address_type = "Shipping"      # Set as Shipping type for customers
        address_doc.address_line1 = address_line1[:240]  # Respect field length limit
        
        if address_line2:
            address_doc.address_line2 = address_line2[:240]
        
        address_doc.city = city
        address_doc.state = state
        address_doc.country = country
        address_doc.pincode = pincode
        
        # Set coordinates
        address_doc.custom_latitude = coordinates['latitude']
        address_doc.custom_longitude = coordinates['longitude']
        
        # Add link to Customer
        address_doc.append("links", {
            "link_doctype": "Customer",
            "link_name": customer_name
        })
        
        # Set as primary and shipping address preference
        address_doc.is_primary_address = 1
        address_doc.is_shipping_address = 1
        
        # Insert the address record
        address_doc.insert()
        
        print(f"Created address record: {address_doc.name} for customer: {customer_name}")
        return address_doc.name
        
    except Exception as e:
        print(f"Error creating address record for customer {mapping.get('internal_reference')}: {str(e)}")
        frappe.log_error(
            title=f"Customer Address Record Creation Error - {mapping.get('internal_reference')}",
            message=f"Error: {str(e)}\nMapping: {mapping}\nAddress Data: {address_data}"
        )
        return None


def update_customer_addresses(customer_name: str, address_name: str) -> bool:
    """
    Update Customer with the created primary and shipping address.
    
    Args:
        customer_name: Name of the Customer record
        address_name: Name of the created Address record
    
    Returns:
        True if successful, False otherwise
    """
    try:
        from frappe.contacts.doctype.address.address import get_address_display
        
        customer_doc = frappe.get_doc("Customer", customer_name)
        
        # Set primary address
        customer_doc.customer_primary_address = address_name
        
        # Generate and set primary address display
        address_display = get_address_display(address_name)
        customer_doc.primary_address = address_display
        
        # Set custom shipping address
        customer_doc.custom_customer_shipping_address = address_name
        
        customer_doc.save()
        
        print(f"Updated customer {customer_name} with primary address and custom shipping address {address_name}")
        return True
        
    except Exception as e:
        print(f"Error updating customer {customer_name} with address: {str(e)}")
        frappe.log_error(
            title=f"Customer Address Update Error - {customer_name}",
            message=f"Error: {str(e)}\nAddress: {address_name}\nTraceback: {frappe.get_traceback()}"
        )
        return False


@frappe.whitelist()
def run_create_addresses_for_b2b_customers():
    """
    API endpoint to manually trigger address creation for B2B customers.
    """
    if not frappe.has_permission("Address", "create"):
        frappe.throw("Not permitted to create Address records")
    
    result = create_addresses_for_b2b_customers()
    return result

# Order in which we run the functions
# the below function runs first, creating all external mappings
# bench execute "inv_mgmt.cron_functions.new_customers_from_orders.run_new_customers_from_orders"

# then we run this, creating actual customers from those external mappings
# bench execute "inv_mgmt.cron_functions.new_customers_from_orders.run_create_customers_from_external_mappings"

# finally, we run this, creating addresses for the customers
# bench execute "inv_mgmt.cron_functions.new_customers_from_orders.run_create_addresses_for_b2b_customers"