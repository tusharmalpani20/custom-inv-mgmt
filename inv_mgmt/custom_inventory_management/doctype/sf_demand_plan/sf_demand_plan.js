// Copyright (c) 2025, Hopnet Communications LLP and contributors
// For license information, please see license.txt

// frappe.ui.form.on("SF Demand Plan", {
// 	refresh(frm) {

// 	},
// });

frappe.ui.form.on('SF Demand Plan', {
    async onload(frm) {
        console.log('SF Demand Plan onload - planning_week:', frm.doc.planning_week);
        console.log('SF Demand Plan onload - existing entries:', frm.doc.sku_demand_entries);
        
        // Add global CSS to fix z-index issues
        addGlobalCSS();
        
        await build_roster_table(frm);
        
        // Ensure any existing dropdowns are closed
        closeAllDropdowns();
    },

    async refresh(frm) {
        console.log('SF Demand Plan refresh - planning_week:', frm.doc.planning_week);
        console.log('SF Demand Plan refresh - existing entries:', frm.doc.sku_demand_entries);
        
        // Close any open dropdowns before rebuilding
        closeAllDropdowns();
        
        frm.fields_dict.roster_table.$wrapper.empty();
        await build_roster_table(frm);

        // Disable inputs only if document is submitted
        if (frm.doc.docstatus === 1) {
            frm.fields_dict.roster_table.$wrapper.find('input').prop('disabled', true);
        }
    },

    // Add a method to reload the table when child table data changes
    sku_demand_entries_add(frm, cdt, cdn) {
        console.log('Child table entry added:', cdt, cdn);
        // Refresh the table to show the new data
        setTimeout(() => build_roster_table(frm), 100);
    },

    sku_demand_entries_remove(frm, cdt, cdn) {
        console.log('Child table entry removed:', cdt, cdn);
        // Refresh the table to reflect the removal
        setTimeout(() => build_roster_table(frm), 100);
    }
});

// Function to add global CSS for fixing z-index issues
function addGlobalCSS() {
    if (!document.getElementById('demand-plan-css')) {
        const style = document.createElement('style');
        style.id = 'demand-plan-css';
        style.textContent = `
            .demand-plan-container {
                position: relative !important;
                z-index: 1 !important;
            }
            .demand-plan-table {
                position: relative !important;
                z-index: 2 !important;
            }
            .demand-plan-table th,
            .demand-plan-table td {
                position: relative !important;
                z-index: 3 !important;
            }
            .demand-plan-input {
                position: relative !important;
                z-index: 4 !important;
            }
            /* Ensure dropdowns don't block the interface */
            .dropdown-menu,
            .select2-dropdown,
            .modal-backdrop {
                z-index: 9999 !important;
            }
            /* Hide any blocking overlays */
            .ui-blocking-overlay {
                display: none !important;
            }
        `;
        document.head.appendChild(style);
    }
}

// Function to close all dropdowns and modals
function closeAllDropdowns() {
    $('.dropdown-menu').hide();
    $('.select2-dropdown').hide();
    $('.modal-backdrop').hide();
    $('.ui-blocking-overlay').hide();
    
    // Also close any Frappe-specific dropdowns
    $('.frappe-dropdown').hide();
    $('.frappe-autocomplete-dropdown').hide();
    
    // Remove any blocking overlays
    $('.blocking-overlay').remove();
}

async function build_roster_table(frm) {
    const startDate = frappe.datetime.str_to_obj(frm.doc.planning_week);
    if (!startDate) {
        console.log('No planning week date found, returning early');
        return;
    }

    console.log('Building roster table for planning week:', frm.doc.planning_week);

    // Clear any default margins from the wrapper and add proper classes
    frm.fields_dict.roster_table.$wrapper.css({
        'margin': '0',
        'padding': '0',
        'position': 'relative',
        'z-index': '1'
    }).addClass('demand-plan-container');

    // Fetch SKU list
    const skuList = await frappe.call({
        method: 'inv_mgmt.custom_inventory_management.api_end_points.item_api.get_sku_items',
        type: 'GET'
    });

    console.log('Fetched SKU list:', skuList.message?.length || 0, 'items');

    // Fetch holidays
    const holidays = await frappe.call({
        method: 'inv_mgmt.custom_inventory_management.api_end_points.item_api.get_holidays_for_week',
        args: {
            start_date: frappe.datetime.add_days(frm.doc.planning_week, 0),
            end_date: frappe.datetime.add_days(frm.doc.planning_week, 6)
        }
    });

    const holidayMap = {};
    (holidays.message || []).forEach(h => {
        holidayMap[frappe.datetime.str_to_obj(h.holiday_date).toDateString()] = h.description;
    });

    // Prepare a map from existing child table entries
    const demandMap = {};
    console.log('Processing existing demand entries:', frm.doc.sku_demand_entries?.length || 0);
    
    (frm.doc.sku_demand_entries || []).forEach(row => {
        const key = `${row.item_code}_${row.day_index}`;
        demandMap[key] = row.quantity;
        console.log(`Mapping demand entry: ${key} = ${row.quantity}`);
    });

    console.log('Final demand map:', demandMap);

    // Build the HTML table with improved z-index and positioning
    let table = `<div style="margin-bottom: 15px; position: relative; z-index: 1;">
        <div class="input-group" style="max-width: 400px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-radius: 6px; overflow: hidden; border: 1px solid #e9ecef; position: relative; z-index: 2;">
            <span class="input-group-addon" style="background: #f8f9fa; border: none; padding: 12px 16px; display: flex; align-items: center; justify-content: center;">
                <i class="fa fa-search" style="color: #6c757d;"></i>
            </span>
            <input type="text" class="form-control demand-plan-input" id="sku-search" placeholder="Search SKUs..." 
                style="border: none; padding: 12px 16px; font-size: 14px; background: white; height: auto;">
        </div>
    </div>
    <div class="table-container demand-plan-table" style="max-height: 700px; overflow-y: auto; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); border: 1px solid #e9ecef; background: white; margin-top: 0; position: relative; z-index: 2;">
        <table class="table demand-plan-table" style="font-size: 13px; margin: 0; width: 100%; border-collapse: collapse;">
        <thead style="position: sticky; top: 0; background: #f8f9fa; z-index: 10;">
        <tr style="background: #f8f9fa; margin: 0;">
        <th style="position: sticky; left: 0; background: #f8f9fa; z-index: 11; border-right: 1px solid #dee2e6; min-width: 180px; padding: 8px; color: #495057; font-weight: 600; text-align: left; margin: 0;">SKU</th>`;
    
    for (let i = 0; i < 7; i++) {
        const dateObj = frappe.datetime.str_to_obj(frappe.datetime.add_days(frm.doc.planning_week, i));
        const dateStr = frappe.datetime.obj_to_user(dateObj);
        const dayStr = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][dateObj.getDay()];
        table += `<th style="background: #f8f9fa; min-width: 100px; padding: 8px; color: #495057; font-weight: 600; text-align: center; margin: 0;">${dayStr}<br><small style="color: #6c757d;">${dateStr}</small></th>`;
    }
    table += `</tr><tr style="background: #ffffff; margin: 0;" class="holiday-row"><td style="position: sticky; left: 0; background: #ffffff; z-index: 11; border-right: 1px solid #dee2e6; padding: 6px; font-weight: 600; color: #495057; margin: 0;">Holiday</td>`;

    for (let i = 0; i < 7; i++) {
        const d = frappe.datetime.str_to_obj(frappe.datetime.add_days(frm.doc.planning_week, i)).toDateString();
        table += `<td style="background: #ffffff; padding: 6px; text-align: center; margin: 0;"><small style="color: #dc3545; font-weight: 500;">${holidayMap[d] || ''}</small></td>`;
    }

    table += `</tr></thead><tbody>`;

    // Body
    skuList.message.forEach((item, index) => {
        const rowBg = index % 2 === 0 ? '#ffffff' : '#f8f9fa';
        table += `<tr class="sku-row" data-sku="${item.item_code.toLowerCase()}" data-name="${item.item_name.toLowerCase()}" style="background: ${rowBg}; margin: 0;">
        <td style="position: sticky; left: 0; background: ${rowBg}; z-index: 1; border-right: 1px solid #dee2e6; padding: 8px; min-width: 180px; margin: 0;">
            <div style="font-weight: 600; color: #495057; margin-bottom: 2px; font-size: 12px;">${item.item_name}</div>
            <div style="font-size: 11px; color: #6c757d;">${item.item_code}</div>
        </td>`;
        for (let i = 0; i < 7; i++) {
            const key = `${item.item_code}_${i}`;
            const val = demandMap[key] ?? '';
            const disabled = (frm.doc.docstatus === 1) ? 'disabled' : '';
            table += `<td style="padding: 6px; min-width: 100px; text-align: center; vertical-align: middle; margin: 0;">
                <div style="display: flex; justify-content: center; align-items: center;">
                    <input type="number" class="form-control sku-input demand-plan-input" 
                    data-item="${item.item_code}" data-day="${i}" value="${val}" min="0" 
                    style="width: 70px; height: 28px; font-size: 12px; border: 1px solid #ced4da; border-radius: 4px; text-align: center; transition: all 0.2s ease; background: white; margin: 0;" ${disabled}>
                </div>
            </td>`;
        }
        table += `</tr>`;
    });

    table += `</tbody></table></div>`;
    frm.fields_dict.roster_table.$wrapper.html(table);

    // Add click-outside handler to close any open dropdowns
    $(document).off('click.demand-plan').on('click.demand-plan', function(e) {
        if (!$(e.target).closest('.table-container, .input-group, .dropdown-menu, .select2-container, .demand-plan-container').length) {
            closeAllDropdowns();
        }
    });

    // Add hover effects and focus styles
    frm.fields_dict.roster_table.$wrapper.find('.sku-input').on('focus', function() {
        $(this).css({
            'border-color': '#80bdff',
            'box-shadow': '0 0 0 0.2rem rgba(0, 123, 255, 0.25)',
            'outline': 'none'
        });
    }).on('blur', function() {
        $(this).css({
            'border-color': '#ced4da',
            'box-shadow': 'none'
        });
    }).on('hover', function() {
        $(this).css('border-color', '#adb5bd');
    });

    // Add search functionality
    frm.fields_dict.roster_table.$wrapper.find('#sku-search').on('input', function() {
        const searchTerm = $(this).val().toLowerCase();
        const rows = frm.fields_dict.roster_table.$wrapper.find('.sku-row');
        
        rows.each(function() {
            const sku = $(this).data('sku');
            const name = $(this).data('name');
            const matches = sku.includes(searchTerm) || name.includes(searchTerm);
            
            if (searchTerm === '' || matches) {
                $(this).show();
            } else {
                $(this).hide();
            }
        });
    });

    // Change handler to update child table
    frm.fields_dict.roster_table.$wrapper.find('.sku-input').on('change', function () {
        const item_code = $(this).data('item');
        const day_index = parseInt($(this).data('day'));
        const quantity = parseFloat($(this).val()) || 0;

        console.log(`Input changed: ${item_code}_${day_index} = ${quantity}`);

        const existing = (frm.doc.sku_demand_entries || []).find(
            row => row.item_code === item_code && row.day_index === day_index
        );

        if (existing) {
            existing.quantity = quantity;
            console.log(`Updated existing entry: ${item_code}_${day_index} = ${quantity}`);
        } else {
            frm.add_child('sku_demand_entries', {
                item_code,
                day_index,
                quantity
            });
            console.log(`Added new entry: ${item_code}_${day_index} = ${quantity}`);
        }

        // Mark the form as dirty so Save becomes active
        frm.dirty();
    });

    // Clean up event handlers when form is destroyed
    frm.fields_dict.roster_table.$wrapper.on('remove', function() {
        $(document).off('click.demand-plan');
    });
}
