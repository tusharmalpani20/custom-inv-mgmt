// SF Indent Master List View Scripts
console.log("SF Indent Master List View JavaScript file loaded successfully");

frappe.listview_settings['SF Indent Master'] = {
	onload: function (listview) {
		console.log("SF Indent Master list view onload triggered");
		console.log("Current user:", frappe.user);
		console.log("User roles:", frappe.user_roles);
		console.log("Listview object:", listview);
		console.log("Listview page object:", listview.page);
		
		// Try adding button in the main toolbar
		try {
			listview.page.add_inner_button(__("Generate Adjusted Indents"), function () {
				console.log("Generate Adjusted Indents button clicked");
				show_adjusted_indent_modal(listview);
			})
			.addClass("btn-warning").css({'color':'darkred','font-weight': 'normal'});
			console.log("Generate Adjusted Indents button added to inner toolbar");
		} catch (error) {
			console.error("Error adding inner button:", error);
		}
		
		// Also try adding to actions dropdown
		try {
			listview.page.add_action_item(__("Generate Adjusted Indents"), function () {
				console.log("Generate Adjusted Indents action item clicked");
				show_adjusted_indent_modal(listview);
			});
			console.log("Generate Adjusted Indents action item added to dropdown");
		} catch (error) {
			console.error("Error adding action item:", error);
		}
	}
};

function show_adjusted_indent_modal(listview) {
	let d = new frappe.ui.Dialog({
		title: __('Generate Adjusted Indents'),
		size: 'medium',
		fields: [
			{
				fieldname: 'message',
				fieldtype: 'HTML',
				options: `
					<div class="alert alert-info">
						Do you want to generate adjusted indents for shortfall?
						<br><br>
						This will create adjusted indents for all routes where sales orders exceed indent quantities.
					</div>
				`
			}
		],
		primary_action_label: __('Yes, Generate'),
		primary_action: function() {
			generate_adjusted_indents(d);
		},
		secondary_action_label: __('Cancel'),
		secondary_action: function() {
			d.hide();
		}
	});

	d.show();
}

function generate_adjusted_indents(dialog) {
	frappe.dom.freeze(__('Generating Adjusted Indents...')); // Show loading indicator
	console.log("Generating Adjusted Indents...");
	frappe.call({
		method: 'inv_mgmt.custom_inventory_management.api_end_points.indent.create_adjusted_indents_for_shortfall',
		callback: function(r) {
			frappe.dom.unfreeze(); // Hide loading indicator
			
			if (r.message && r.message.success) {
				show_result_dialog(r.message);
			} else {
				frappe.msgprint(__('Error: {0}', [r.message ? r.message.message : 'Unknown error occurred']));
			}
			dialog.hide();
		}
	});
}

function show_result_dialog(result) {
	let details_html = '';
	
	if (result.data && result.data.details && result.data.details.length > 0) {
		details_html = `
			<div class="alert alert-info">
				<h4>Processing Details</h4>
				<table class="table table-bordered">
					<thead>
						<tr>
							<th>Indent</th>
							<th>Status</th>
							<th>Message</th>
						</tr>
					</thead>
					<tbody>
						${result.data.details.map(detail => `
							<tr>
								<td>${detail.indent_name || 'N/A'}</td>
								<td>${detail.status || 'N/A'}</td>
								<td>${detail.message || 'N/A'}</td>
							</tr>
						`).join('')}
					</tbody>
				</table>
			</div>
		`;
	}

	let summary_html = `
		<div class="alert alert-success">
			<h4>Summary</h4>
			<ul>
				<li><b>${result.message}</b></li>
				<li>Total Processed: ${result.data ? result.data.processed_indents : 0}</li>
				<li>Adjusted Indents Created: ${result.data ? result.data.created_adjusted_indents : 0}</li>
				<li>Indents with Shortfall: ${result.data ? result.data.indents_with_shortfall : 0}</li>
				<li>Indents without Shortfall: ${result.data ? result.data.indents_without_shortfall : 0}</li>
				<li>Already Adjusted: ${result.data ? result.data.already_adjusted_indents : 0}</li>
			</ul>
		</div>
	`;

	let result_dialog = new frappe.ui.Dialog({
		title: __('Adjusted Indent Generation Results'),
		size: 'large',
		fields: [
			{ fieldname: 'summary', fieldtype: 'HTML', options: summary_html },
			{ fieldname: 'details', fieldtype: 'HTML', options: details_html }
		],
		primary_action_label: __('Close'),
		primary_action: function() {
			result_dialog.hide();
		}
	});

	result_dialog.show();
}
