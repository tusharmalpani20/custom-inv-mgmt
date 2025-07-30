// Copyright (c) 2025, Hopnet Communications LLP and contributors
// For license information, please see license.txt

// Global variable to track current drop_type
let current_drop_type = null;

frappe.ui.form.on("SF Delivery Route Master", {
	refresh(frm) {
		// Filter drop_type options to only Customer and Warehouse
		frm.fields_dict.delivery_points.grid.get_field('drop_type').get_query = function() {
			return {
				filters: {
					'name': ['in', ['Customer', 'Warehouse']]
				}
			};
		};
		
		// Filter drop_point based on drop_type
		frm.fields_dict.delivery_points.grid.get_field('drop_point').get_query = function() {
			if (current_drop_type === 'Warehouse') {
				return {
					filters: {
						'custom_warehouse_category': ['in', ['Plant', 'Distribution Center', 'Darkstore']]
					}
				};
			}
			return {};
		};
	},
});

// Track drop_type changes
frappe.ui.form.on("SF Delivery Point", {
	drop_type: function(frm, cdt, cdn) {
		let row = frappe.get_doc(cdt, cdn);
		current_drop_type = row.drop_type;
	}
});
