// Copyright (c) 2025, Hopnet Communications LLP and contributors
// For license information, please see license.txt

frappe.ui.form.on('SF Demand Plan', {
    async onload(frm) {
        console.log('SF Demand Plan onload - planning_week:', frm.doc.planning_week);
        console.log('SF Demand Plan onload - existing entries:', frm.doc.sku_demand_entries);
        
        await build_roster_table(frm);
        
        // Ensure action buttons are visible
        ensureActionButtonsVisible(frm);
    },

    async refresh(frm) {
        console.log('SF Demand Plan refresh - planning_week:', frm.doc.planning_week);
        console.log('SF Demand Plan refresh - existing entries:', frm.doc.sku_demand_entries);
        
        frm.fields_dict.roster_table.$wrapper.empty();
        await build_roster_table(frm);

        // Disable inputs only if document is submitted
        if (frm.doc.docstatus === 1) {
            frm.fields_dict.roster_table.$wrapper.find('input').prop('disabled', true);
        }
        
        // Ensure action buttons are visible
        ensureActionButtonsVisible(frm);
        
        // Additional check for action buttons after a delay
        setTimeout(() => {
            forceShowActionButtons();
        }, 500);
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

// Function to ensure action buttons are visible and not blocked
function ensureActionButtonsVisible(frm) {
    // Force show action buttons
    setTimeout(() => {
        // Show any hidden action buttons
        $('.form-actions').show();
        $('.btn-primary').show();
        $('.btn-secondary').show();
        $('.btn-default').show();
        
        // Remove any blocking overlays
        $('.modal-backdrop').remove();
        $('.ui-blocking-overlay').remove();
        $('.blocking-overlay').remove();
        
        // Ensure proper z-index for action buttons
        $('.form-actions').css('z-index', '9999');
        $('.btn-primary').css('z-index', '9999');
        $('.btn-secondary').css('z-index', '9999');
        
        // Add comprehensive CSS to fix all issues
        if (!document.getElementById('demand-plan-fixes')) {
            const style = document.createElement('style');
            style.id = 'demand-plan-fixes';
            style.textContent = `
                /* Table container fixes */
                .table-container {
                    max-height: 500px !important;
                    overflow-y: auto !important;
                    margin-bottom: 30px !important;
                    position: relative !important;
                }
                
                /* Sticky header fixes - HIGHEST PRIORITY */
                .table-container thead {
                    position: sticky !important;
                    top: 0 !important;
                    z-index: 1000 !important;
                    background: #f8f9fa !important;
                }
                
                .table-container thead th {
                    position: sticky !important;
                    top: 0 !important;
                    z-index: 1001 !important;
                    background: #f8f9fa !important;
                }
                
                .table-container thead th[style*="position: sticky"] {
                    z-index: 1002 !important;
                    background: #f8f9fa !important;
                }
                
                /* Holiday row fixes */
                .table-container .holiday-row td {
                    position: sticky !important;
                    top: 40px !important;
                    z-index: 1001 !important;
                    background: #ffffff !important;
                }
                
                .table-container .holiday-row td[style*="position: sticky"] {
                    z-index: 1002 !important;
                    background: #ffffff !important;
                }
                
                /* Data row sticky column fixes */
                .table-container tbody tr td[style*="position: sticky"] {
                    z-index: 1000 !important;
                }
                
                /* Input field fixes - LOWEST PRIORITY */
                .table-container input[type="number"],
                .table-container input[type="text"] {
                    position: relative !important;
                    z-index: 1 !important;
                }
                
                /* Form actions fixes */
                .form-actions {
                    position: relative !important;
                    z-index: 9999 !important;
                    background: white !important;
                    padding: 10px !important;
                    border-top: 1px solid #e9ecef !important;
                    margin-top: 20px !important;
                }
                
                .btn-primary, .btn-secondary, .btn-default {
                    position: relative !important;
                    z-index: 10000 !important;
                }
                
                /* Ensure table doesn't block anything */
                .table-container table {
                    position: relative !important;
                    z-index: 1 !important;
                }
                
                .table-container tbody {
                    position: relative !important;
                    z-index: 1 !important;
                }
                
                /* Remove any problematic overlays */
                .modal-backdrop,
                .ui-blocking-overlay,
                .blocking-overlay {
                    display: none !important;
                }
            `;
            document.head.appendChild(style);
        }
        
        console.log('Action buttons visibility ensured');
    }, 100);
}

// Function to force show action buttons with multiple approaches
function forceShowActionButtons() {
    console.log('Force showing action buttons...');
    
    // Method 1: Direct jQuery manipulation
    $('.form-actions').show().css({
        'display': 'block !important',
        'visibility': 'visible !important',
        'opacity': '1 !important',
        'z-index': '99999 !important',
        'position': 'relative !important'
    });
    
    $('.btn-primary, .btn-secondary, .btn-default').show().css({
        'display': 'inline-block !important',
        'visibility': 'visible !important',
        'opacity': '1 !important',
        'z-index': '100000 !important',
        'position': 'relative !important'
    });
    
    // Method 2: Remove any blocking elements
    $('.modal-backdrop, .ui-blocking-overlay, .blocking-overlay, .overlay').remove();
    
    // Method 3: Force Frappe to show action buttons
    if (window.frappe && window.frappe.ui && window.frappe.ui.form) {
        const currentForm = window.frappe.ui.form.get_cur_frm();
        if (currentForm) {
            currentForm.show_actions();
        }
    }
    
    // Method 4: Add CSS to ensure visibility
    if (!document.getElementById('force-action-buttons')) {
        const style = document.createElement('style');
        style.id = 'force-action-buttons';
        style.textContent = `
            .form-actions {
                display: block !important;
                visibility: visible !important;
                opacity: 1 !important;
                z-index: 99999 !important;
                position: relative !important;
                background: white !important;
                padding: 10px !important;
                border-top: 1px solid #e9ecef !important;
                margin-top: 20px !important;
            }
            .btn-primary, .btn-secondary, .btn-default {
                display: inline-block !important;
                visibility: visible !important;
                opacity: 1 !important;
                z-index: 100000 !important;
                position: relative !important;
            }
        `;
        document.head.appendChild(style);
    }
    
    console.log('Action buttons force show completed');
}

async function build_roster_table(frm) {
    const startDate = frappe.datetime.str_to_obj(frm.doc.planning_week);
    if (!startDate) {
        console.log('No planning week date found, returning early');
        return;
    }

    console.log('Building roster table for planning week:', frm.doc.planning_week);

    // Clear any default margins from the wrapper
    frm.fields_dict.roster_table.$wrapper.css({
        'margin': '0',
        'padding': '0',
        'position': 'relative'
    });

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

    // Build the HTML table with proper positioning
    let table = `<div style="margin-bottom: 15px;">
        <div class="input-group" style="max-width: 400px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-radius: 6px; overflow: hidden; border: 1px solid #e9ecef;">
            <span class="input-group-addon" style="background: #f8f9fa; border: none; padding: 12px 16px; display: flex; align-items: center; justify-content: center;">
                <i class="fa fa-search" style="color: #6c757d;"></i>
            </span>
            <input type="text" class="form-control" id="sku-search" placeholder="Search SKUs..." 
                style="border: none; padding: 12px 16px; font-size: 14px; background: white; height: auto;">
        </div>
    </div>
    <div class="table-container" style="max-height: 500px; overflow-y: auto; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); border: 1px solid #e9ecef; background: white; margin-bottom: 30px;">
        <table class="table" style="font-size: 13px; margin: 0; width: 100%; border-collapse: collapse;">
        <thead style="position: sticky; top: 0; background: #f8f9fa;">
        <tr style="background: #f8f9fa; margin: 0;">
        <th style="position: sticky; left: 0; background: #f8f9fa; border-right: 1px solid #dee2e6; min-width: 180px; padding: 8px; color: #495057; font-weight: 600; text-align: left; margin: 0;">SKU</th>`;
    
    for (let i = 0; i < 7; i++) {
        const dateObj = frappe.datetime.str_to_obj(frappe.datetime.add_days(frm.doc.planning_week, i));
        const dateStr = frappe.datetime.obj_to_user(dateObj);
        const dayStr = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][dateObj.getDay()];
        table += `<th style="background: #f8f9fa; min-width: 100px; padding: 8px; color: #495057; font-weight: 600; text-align: center; margin: 0;">${dayStr}<br><small style="color: #6c757d;">${dateStr}</small></th>`;
    }
    table += `</tr><tr style="background: #ffffff; margin: 0;" class="holiday-row"><td style="position: sticky; left: 0; background: #ffffff; border-right: 1px solid #dee2e6; padding: 6px; font-weight: 600; color: #495057; margin: 0;">Holiday</td>`;

    for (let i = 0; i < 7; i++) {
        const d = frappe.datetime.str_to_obj(frappe.datetime.add_days(frm.doc.planning_week, i)).toDateString();
        table += `<td style="background: #ffffff; padding: 6px; text-align: center; margin: 0;"><small style="color: #dc3545; font-weight: 500;">${holidayMap[d] || ''}</small></td>`;
    }

    table += `</tr></thead><tbody>`;

    // Body
    skuList.message.forEach((item, index) => {
        const rowBg = index % 2 === 0 ? '#ffffff' : '#f8f9fa';
        table += `<tr class="sku-row" data-sku="${item.item_code.toLowerCase()}" data-name="${item.item_name.toLowerCase()}" style="background: ${rowBg}; margin: 0;">
        <td style="position: sticky; left: 0; background: ${rowBg}; border-right: 1px solid #dee2e6; padding: 8px; min-width: 180px; margin: 0;">
            <div style="font-weight: 600; color: #495057; margin-bottom: 2px; font-size: 12px;">${item.item_name}</div>
            <div style="font-size: 11px; color: #6c757d;">${item.item_code}</div>
        </td>`;
        for (let i = 0; i < 7; i++) {
            const key = `${item.item_code}_${i}`;
            const val = demandMap[key] ?? '';
            const disabled = (frm.doc.docstatus === 1) ? 'disabled' : '';
            table += `<td style="padding: 6px; min-width: 100px; text-align: center; vertical-align: middle; margin: 0;">
                <div style="display: flex; justify-content: center; align-items: center;">
                    <input type="number" class="form-control sku-input" 
                    data-item="${item.item_code}" data-day="${i}" value="${val}" min="0" 
                    style="width: 70px; height: 28px; font-size: 12px; border: 1px solid #ced4da; border-radius: 4px; text-align: center; transition: all 0.2s ease; background: white; margin: 0;" ${disabled}>
                </div>
            </td>`;
        }
        table += `</tr>`;
    });

    table += `</tbody></table></div>`;
    frm.fields_dict.roster_table.$wrapper.html(table);

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
}

// Function to start monitoring action buttons
function startActionButtonMonitor() {
    console.log('Starting action button monitor...');
    
    // Clear any existing interval
    if (window.actionButtonInterval) {
        clearInterval(window.actionButtonInterval);
    }
    
    // Check every 2 seconds for action buttons
    window.actionButtonInterval = setInterval(() => {
        const formActions = $('.form-actions');
        const actionButtons = $('.btn-primary, .btn-secondary, .btn-default');
        
        // If action buttons are missing or hidden, force show them
        if (formActions.length === 0 || formActions.is(':hidden') || 
            actionButtons.length === 0 || actionButtons.is(':hidden')) {
            console.log('Action buttons missing or hidden, forcing show...');
            forceShowActionButtons();
        }
        
        // Also check if they're behind other elements
        if (formActions.length > 0) {
            const zIndex = parseInt(formActions.css('z-index')) || 0;
            if (zIndex < 9999) {
                console.log('Action buttons z-index too low, fixing...');
                forceShowActionButtons();
            }
        }
    }, 2000);
    
    // Also check on various events
    $(document).on('scroll.action-buttons', forceShowActionButtons);
    $(document).on('click.action-buttons', forceShowActionButtons);
    $(window).on('resize.action-buttons', forceShowActionButtons);
    
    console.log('Action button monitor started');
}

// Function to cleanup event listeners and intervals
function cleanupActionButtonMonitor() {
    console.log('Cleaning up action button monitor...');
    
    if (window.actionButtonInterval) {
        clearInterval(window.actionButtonInterval);
        window.actionButtonInterval = null;
    }
    
    $(document).off('scroll.action-buttons');
    $(document).off('click.action-buttons');
    $(window).off('resize.action-buttons');
    
    console.log('Action button monitor cleaned up');
}

// Global document ready handler to ensure action buttons are always visible
$(document).ready(function() {
    console.log('Document ready - ensuring action buttons are visible...');
    
    // Wait a bit for Frappe to load
    setTimeout(() => {
        forceShowActionButtons();
        startActionButtonMonitor();
    }, 1000);
    
    // Also check when the page becomes visible
    $(document).on('visibilitychange', function() {
        if (!document.hidden) {
            console.log('Page became visible - checking action buttons...');
            forceShowActionButtons();
        }
    });
    
    // Check on any DOM changes
    const observer = new MutationObserver(function(mutations) {
        mutations.forEach(function(mutation) {
            if (mutation.type === 'childList') {
                // Check if action buttons were removed
                const formActions = $('.form-actions');
                if (formActions.length === 0) {
                    console.log('Action buttons removed from DOM - forcing show...');
                    setTimeout(forceShowActionButtons, 100);
                }
            }
        });
    });
    
    // Start observing
    observer.observe(document.body, {
        childList: true,
        subtree: true
    });
    
    console.log('Global action button monitoring started');
});
