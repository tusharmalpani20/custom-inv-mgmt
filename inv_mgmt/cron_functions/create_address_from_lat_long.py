import frappe
import requests
import time
from typing import Dict, Any, List, Optional
from frappe.utils import cstr

# Rate limit for Nominatim API - 1 request per second
NOMINATIM_API_DELAY_SECONDS = 1

def create_address_from_lat_long_for_sf_facility_master():
    """
    Main cron function to create Address records for SF Facility Master documents 
    that don't have shipping_address set but have latitude and longitude coordinates.
    
    Process:
    1. Get SF Facility Master records without shipping_address but with coordinates
    2. For each facility, call Nominatim API to get address details
    3. Create Address record with proper naming and linking
    4. Update SF Facility Master with the new shipping_address
    5. Respects 1 second rate limit between API calls
    """
    try:
        print("Starting address creation from latitude/longitude process...")
        
        # Get SF Facility Master records that need address creation
        facilities = get_facilities_needing_addresses()
        
        if not facilities:
            print("No facilities found that need address creation")
            return {
                "success": True,
                "message": "No facilities found that need address creation",
                "processed": 0
            }
        
        print(f"Found {len(facilities)} facilities that need address creation")
        
        # Start transaction for all address creation
        frappe.db.begin()
        
        success_count = 0
        error_count = 0
        errors = []
        
        for facility in facilities:
            try:
                print(f"Processing facility: {facility.name} - {facility.facility_name}")
                
                # Get address data from Nominatim API
                address_data = get_address_from_nominatim(facility.latitude, facility.longitude)
                
                if address_data:
                    # Create Address record
                    address_name = create_address_record(facility, address_data)
                    
                    if address_name:
                        # Update SF Facility Master with shipping address
                        update_facility_shipping_address(facility.name, address_name)
                        success_count += 1
                        print(f"Successfully created address for facility: {facility.name}")
                    else:
                        error_count += 1
                        errors.append({
                            "facility": facility.name,
                            "error": "Failed to create address record"
                        })
                else:
                    error_count += 1
                    errors.append({
                        "facility": facility.name,
                        "error": "Failed to get address data from Nominatim API"
                    })
                
                # Rate limiting - wait 1 second between API calls
                if len(facilities) > 1:  # Only wait if there are more facilities to process
                    print(f"Waiting {NOMINATIM_API_DELAY_SECONDS} seconds before next API call...")
                    time.sleep(NOMINATIM_API_DELAY_SECONDS)
                
            except Exception as e:
                error_count += 1
                error_detail = {
                    "facility": facility.name,
                    "error": str(e)
                }
                errors.append(error_detail)
                
                print(f"Error processing facility {facility.name}: {str(e)}")
                frappe.log_error(
                    title=f"Address Creation Error - {facility.name}",
                    message=f"Error: {str(e)}\nFacility Data: {facility}"
                )
        
        # Commit transaction
        frappe.db.commit()
        
        print(f"Address creation completed. Success: {success_count}, Errors: {error_count}")
        
        return {
            "success": True,
            "message": f"Address creation completed. Success: {success_count}, Errors: {error_count}",
            "processed": success_count,
            "errors": errors if errors else None
        }
        
    except Exception as e:
        frappe.db.rollback()
        print(f"Critical error during address creation: {str(e)}")
        frappe.log_error(
            title="Address Creation from Lat/Long - Critical Error",
            message=f"Error: {str(e)}\nTraceback: {frappe.get_traceback()}"
        )
        return {
            "success": False,
            "message": f"Critical error during address creation: {str(e)}"
        }


def get_facilities_needing_addresses() -> List[Dict[str, Any]]:
    """
    Get SF Facility Master records that don't have shipping_address set 
    but have latitude and longitude coordinates.
    
    Returns:
        List of facility dictionaries with required fields
    """
    try:
        facilities = frappe.get_all(
            "SF Facility Master",
            filters={
                "shipping_address": ["is", "not set"],  # No shipping address set
                "latitude": ["is", "set"],              # Has latitude
                "longitude": ["is", "set"]              # Has longitude
            },
            fields=["name", "facility_name", "latitude", "longitude", "type"],
            order_by="creation asc"
        )
        
        # Filter out facilities with empty/null or invalid coordinates
        valid_facilities = []
        for facility in facilities:
            if (facility.latitude and facility.longitude and 
                str(facility.latitude).strip() and str(facility.longitude).strip()):
                
                try:
                    lat = float(facility.latitude)
                    lon = float(facility.longitude)
                    
                    # Validate coordinate ranges and check for invalid values
                    if (is_valid_coordinates(lat, lon)):
                        valid_facilities.append(facility)
                    else:
                        print(f"Skipping facility {facility.name} due to invalid coordinates: {lat}, {lon}")
                        
                except (ValueError, TypeError):
                    print(f"Skipping facility {facility.name} due to non-numeric coordinates: {facility.latitude}, {facility.longitude}")
        
        return valid_facilities
        
    except Exception as e:
        frappe.log_error(
            title="Get Facilities Needing Addresses - Error",
            message=f"Error: {str(e)}\nTraceback: {frappe.get_traceback()}"
        )
        return []


def is_valid_coordinates(lat: float, lon: float) -> bool:
    """
    Validate if latitude and longitude coordinates are reasonable.
    
    Args:
        lat: Latitude value
        lon: Longitude value
    
    Returns:
        True if coordinates are valid, False otherwise
    """
    # Basic range validation
    if not (-90 <= lat <= 90):
        return False
    if not (-180 <= lon <= 180):
        return False
    
    # Check for obviously invalid coordinates
    if lat == lon:  # Same value for lat and lon is suspicious
        return False
    
    # Check for common invalid placeholder values
    invalid_coords = [
        (0, 0), (1, 1), (-1, -1), (90, 90), (-90, -90),
        (180, 180), (-180, -180)
    ]
    
    if (lat, lon) in invalid_coords:
        return False
    
    # Check if coordinates are within reasonable bounds for India (assuming this is for Indian facilities)
    # India roughly: Lat 6.75° to 37.08°, Lon 68.03° to 97.39°
    # Adding some buffer for nearby regions
    if not (5 <= lat <= 40):
        return False
    if not (65 <= lon <= 100):
        return False
    
    return True


def get_address_from_nominatim(latitude: str, longitude: str) -> Optional[Dict[str, Any]]:
    """
    Call Nominatim API to get address details from latitude and longitude.
    
    Args:
        latitude: Latitude coordinate as string
        longitude: Longitude coordinate as string
    
    Returns:
        Dictionary containing address details or None if failed
    """
    try:
        # Clean and validate coordinates
        lat = str(latitude).strip()
        lon = str(longitude).strip()
        
        if not lat or not lon:
            print(f"Invalid coordinates: lat={lat}, lon={lon}")
            return None
        
        # Construct API URL
        api_url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lon}&zoom=18&addressdetails=1"
        
        print(f"Calling Nominatim API: {api_url}")
        
        # Make API request with proper headers
        headers = {
            'User-Agent': 'SidFarmERP/1.0 (contact@sidfarm.com)'  # Required by Nominatim usage policy
        }
        
        response = requests.get(api_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        api_data = response.json()
        
        # Check if we got valid address data
        if not api_data or 'error' in api_data:
            print(f"No valid address data for coordinates: {lat}, {lon}")
            return None
        
        print(f"Successfully retrieved address data for coordinates: {lat}, {lon}")
        return api_data
        
    except requests.RequestException as e:
        print(f"API request error for coordinates {latitude}, {longitude}: {str(e)}")
        frappe.log_error(
            title="Nominatim API Request Error",
            message=f"Error: {str(e)}\nCoordinates: {latitude}, {longitude}"
        )
        return None
    except Exception as e:
        print(f"Error getting address from Nominatim: {str(e)}")
        frappe.log_error(
            title="Nominatim Address Extraction Error",
            message=f"Error: {str(e)}\nCoordinates: {latitude}, {longitude}"
        )
        return None


def create_address_record(facility: Dict[str, Any], address_data: Dict[str, Any]) -> Optional[str]:
    """
    Create Address record from Nominatim API data and link it to the SF Facility Master.
    
    Args:
        facility: SF Facility Master data
        address_data: Address data from Nominatim API
    
    Returns:
        Address record name if successful, None otherwise
    """
    try:
        # Extract address components from Nominatim response
        address_components = address_data.get("address", {})
        
        # Map Nominatim fields to Address fields
        address_line1 = get_address_line1(address_components)
        address_line2 = get_address_line2(address_components)
        city = get_city(address_components)
        state = get_state(address_components)
        country = get_country(address_components)
        pincode = get_pincode(address_components)
        
        # Log what we found for debugging
        print(f"Address components for {facility.name}:")
        print(f"  Address Line 1: {address_line1}")
        print(f"  Address Line 2: {address_line2}")
        print(f"  City: {city}")
        print(f"  State: {state}")
        print(f"  Country: {country}")
        print(f"  Pincode: {pincode}")
        
        # Validate required fields
        if not address_line1 or address_line1 == "Location not specified":
            print(f"No valid address line 1 found for facility {facility.name}")
            print(f"Available address components: {list(address_components.keys())}")
            return None
        
        if not city or city == "Unknown City":
            print(f"No valid city found for facility {facility.name}")
            print(f"Available address components: {list(address_components.keys())}")
            return None
        
        if not country:
            print(f"No valid country found for facility {facility.name}")
            print(f"Available address components: {list(address_components.keys())}")
            return None
        
        # Create Address document
        address_doc = frappe.new_doc("Address")
        
        # Set address fields
        address_doc.address_title = facility.name  # Use facility name as address title
        address_doc.address_type = "Shipping"      # Always set as Shipping type
        address_doc.address_line1 = address_line1[:240]  # Respect field length limit
        
        if address_line2:
            address_doc.address_line2 = address_line2[:240]
        
        address_doc.city = city
        address_doc.state = state
        address_doc.country = country
        address_doc.pincode = pincode

        address_doc.custom_latitude = facility.latitude
        address_doc.custom_longitude = facility.longitude
        
        # Add link to SF Facility Master
        address_doc.append("links", {
            "link_doctype": "SF Facility Master",
            "link_name": facility.name
        })
        
        # Set as shipping address preference
        address_doc.is_shipping_address = 1
        
        # Insert the address record
        address_doc.insert()
        
        print(f"Created address record: {address_doc.name} for facility: {facility.name}")
        return address_doc.name
        
    except Exception as e:
        print(f"Error creating address record for facility {facility.name}: {str(e)}")
        frappe.log_error(
            title=f"Address Record Creation Error - {facility.name}",
            message=f"Error: {str(e)}\nFacility: {facility}\nAddress Data: {address_data}"
        )
        return None


def update_facility_shipping_address(facility_name: str, address_name: str) -> bool:
    """
    Update SF Facility Master with the created shipping address.
    
    Args:
        facility_name: Name of the SF Facility Master record
        address_name: Name of the created Address record
    
    Returns:
        True if successful, False otherwise
    """
    try:
        facility_doc = frappe.get_doc("SF Facility Master", facility_name)
        facility_doc.shipping_address = address_name
        facility_doc.save()
        
        print(f"Updated facility {facility_name} with shipping address {address_name}")
        return True
        
    except Exception as e:
        print(f"Error updating facility {facility_name} with shipping address: {str(e)}")
        frappe.log_error(
            title=f"Facility Update Error - {facility_name}",
            message=f"Error: {str(e)}\nAddress: {address_name}"
        )
        return False


def get_address_line1(address_components: Dict[str, Any]) -> str:
    """Extract primary address line from Nominatim address components."""
    # Priority order for address line 1 - start with most specific
    street_fields = [
        "house_number", "building", "shop", "office", 
        "road", "pedestrian", "footway", "cycleway"
    ]
    
    area_fields = [
        "neighbourhood", "suburb", "district", "village", "hamlet",
        "city_district", "municipality", "town", "city"
    ]
    
    admin_fields = [
        "county", "state_district"
    ]
    
    parts = []
    
    # Add house number first if available
    if address_components.get("house_number"):
        parts.append(address_components["house_number"])
    
    # Add road/street name
    for field in ["road", "pedestrian", "footway", "cycleway"]:
        if address_components.get(field):
            parts.append(address_components[field])
            break
    
    # If no street-level data, try area-level identifiers
    if len(parts) <= 1:  # Only house number or nothing
        for field in area_fields:
            if address_components.get(field):
                parts.append(address_components[field])
                break
    
    # If still no useful data, try administrative divisions
    if len(parts) == 0:
        for field in admin_fields:
            if address_components.get(field):
                parts.append(address_components[field])
                break
    
    # Last resort: use display name or a fallback
    if len(parts) == 0:
        display_name = address_components.get("display_name", "")
        if display_name:
            # Take the first meaningful part of display_name
            display_parts = [part.strip() for part in display_name.split(",")]
            # Skip empty parts and find first non-country part
            for part in display_parts:
                if part and part not in ["India", ""]:
                    return part
        
        # Final fallback
        return "Location not specified"
    
    return ", ".join(parts)


def get_address_line2(address_components: Dict[str, Any]) -> Optional[str]:
    """Extract secondary address line from Nominatim address components."""
    # Secondary address components - look for area that wasn't used in line1
    fields_to_try = [
        "neighbourhood", "suburb", "village", "hamlet", 
        "city_district", "municipality"
    ]
    
    for field in fields_to_try:
        if address_components.get(field):
            return address_components[field]
    
    return None


def get_city(address_components: Dict[str, Any]) -> str:
    """Extract city from Nominatim address components."""
    # Priority order for city
    fields_to_try = [
        "city", "town", "municipality", "city_district",
        "county", "state_district", "district"
    ]
    
    for field in fields_to_try:
        if address_components.get(field):
            return address_components[field]
    
    # If no city found, use state as fallback
    if address_components.get("state"):
        return address_components["state"]
    
    return "Unknown City"


def get_state(address_components: Dict[str, Any]) -> Optional[str]:
    """Extract state from Nominatim address components."""
    return address_components.get("state")


def get_country(address_components: Dict[str, Any]) -> str:
    """Extract country from Nominatim address components."""
    return address_components.get("country", "India")  # Default to India


def get_pincode(address_components: Dict[str, Any]) -> Optional[str]:
    """Extract postal code from Nominatim address components."""
    return address_components.get("postcode")


# Utility function for manual testing
@frappe.whitelist()
def test_address_creation_for_facility(facility_name: str) -> Dict[str, Any]:
    """
    Test address creation for a specific SF Facility Master record.
    
    Args:
        facility_name: Name of the SF Facility Master record
    
    Returns:
        Result dictionary with success/error information
    """
    try:
        # Get facility data
        facility = frappe.get_doc("SF Facility Master", facility_name)
        
        if not facility.latitude or not facility.longitude:
            return {
                "success": False,
                "message": "Facility does not have latitude/longitude coordinates"
            }
        
        # Validate coordinates
        try:
            lat = float(facility.latitude)
            lon = float(facility.longitude)
            
            if not is_valid_coordinates(lat, lon):
                return {
                    "success": False,
                    "message": f"Invalid coordinates: {lat}, {lon}"
                }
        except (ValueError, TypeError):
            return {
                "success": False,
                "message": f"Non-numeric coordinates: {facility.latitude}, {facility.longitude}"
            }
        
        # Get address data from API
        address_data = get_address_from_nominatim(facility.latitude, facility.longitude)
        
        if not address_data:
            return {
                "success": False,
                "message": "Failed to get address data from Nominatim API"
            }
        
        # Create address record (without saving to test)
        address_components = address_data.get("address", {})
        
        result = {
            "success": True,
            "message": "Address data retrieved successfully",
            "facility": facility.name,
            "coordinates": f"{facility.latitude}, {facility.longitude}",
            "proposed_address": {
                "address_title": facility.name,
                "address_type": "Shipping",
                "address_line1": get_address_line1(address_components),
                "address_line2": get_address_line2(address_components),
                "city": get_city(address_components),
                "state": get_state(address_components),
                "country": get_country(address_components),
                "pincode": get_pincode(address_components)
            },
            "raw_nominatim_data": address_data
        }
        
        return result
        
    except Exception as e:
        frappe.log_error(
            title=f"Test Address Creation Error - {facility_name}",
            message=f"Error: {str(e)}\nTraceback: {frappe.get_traceback()}"
        )
        return {
            "success": False,
            "message": f"Error during test: {str(e)}"
        }


# New utility function to test coordinates directly
@frappe.whitelist()
def test_coordinates_parsing(latitude: str, longitude: str) -> Dict[str, Any]:
    """
    Test address parsing for specific coordinates.
    
    Args:
        latitude: Latitude coordinate
        longitude: Longitude coordinate
    
    Returns:
        Result dictionary with parsed address components
    """
    try:
        # Validate coordinates
        try:
            lat = float(latitude)
            lon = float(longitude)
            
            if not is_valid_coordinates(lat, lon):
                return {
                    "success": False,
                    "message": f"Invalid coordinates: {lat}, {lon}"
                }
        except (ValueError, TypeError):
            return {
                "success": False,
                "message": f"Non-numeric coordinates: {latitude}, {longitude}"
            }
        
        # Get address data from API
        address_data = get_address_from_nominatim(latitude, longitude)
        
        if not address_data:
            return {
                "success": False,
                "message": "Failed to get address data from Nominatim API"
            }
        
        # Parse address components
        address_components = address_data.get("address", {})
        
        result = {
            "success": True,
            "message": "Address data retrieved and parsed successfully",
            "coordinates": f"{latitude}, {longitude}",
            "parsed_address": {
                "address_line1": get_address_line1(address_components),
                "address_line2": get_address_line2(address_components),
                "city": get_city(address_components),
                "state": get_state(address_components),
                "country": get_country(address_components),
                "pincode": get_pincode(address_components)
            },
            "raw_address_components": address_components,
            "full_nominatim_response": address_data
        }
        
        return result
        
    except Exception as e:
        return {
            "success": False,
            "message": f"Error during coordinate testing: {str(e)}"
        }

# this function is used to create address records for SF Facility Master records that don't have shipping_address set 
# we run this function after SF Facility Master records are created ( and that generally happens when we are importing data for SF Order Master)
# bench execute "inv_mgmt.cron_functions.create_address_from_lat_long.create_address_from_lat_long_for_sf_facility_master"
