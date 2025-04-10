import frappe
from frappe.model.document import Document
from datetime import datetime

class Crate(Document):
    pass

@frappe.whitelist()
def bulk_create_crates(num_crates, branch, date_of_purchase):
    num_crates = int(num_crates)
    
    for i in range(num_crates):
        crate = frappe.new_doc('Crate')
        crate.branch = branch
        crate.date_of_purchase = date_of_purchase
        crate.status = "Available"
        crate.insert(ignore_permissions=True)
        crate.submit()  # Submit the crate after inserting it

    frappe.db.commit()
    return "Successfully created and submitted {} crates".format(num_crates)
