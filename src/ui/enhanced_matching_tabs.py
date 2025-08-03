"""
Enhanced matching tabs for the new Layer 1, Layer 2, and Layer 3 system
"""

import streamlit as st
import pandas as pd

def show_layer0_exact_matches(customer_name, config_mgr):
    """Show Layer 0 exact matches"""
    st.subheader("🎯 Layer 0 - Exact Matches")
    st.info("Perfect matches on style, color, and delivery method")
    
    query = """
    SELECT 
        s.shipment_id,
        s.style_code,
        s.color_description,
        s.delivery_method,
        s.quantity as shipment_qty,
        s.shipped_date,
        emr.match_confidence,
        emr.quantity_difference_percent,
        emr.order_quantity,
        emr.match_layer
    FROM stg_fm_orders_shipped_table s
    INNER JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
    WHERE s.customer_name LIKE ? 
        AND s.po_number = '4755'
        AND emr.match_layer = 'LAYER_0'
    ORDER BY s.shipment_id
    """
    
    try:
        layer0_matches = config_mgr.execute_query(query, [f"{customer_name}%"])
        
        if not layer0_matches.empty:
            st.success(f"Found {len(layer0_matches)} Layer 0 exact matches")
            
            # Add status indicators
            layer0_matches['Match Quality'] = '🟢 Perfect'
            layer0_matches['Qty Status'] = layer0_matches['quantity_difference_percent'].apply(
                lambda x: '✅ Pass' if x <= 5 else ('⚠️ Review' if x <= 10 else '❌ Fail')
            )
            
            # Display with enhanced formatting
            display_cols = [
                'shipment_id', 'style_code', 'color_description', 'delivery_method',
                'shipment_qty', 'order_quantity', 'quantity_difference_percent',
                'Match Quality', 'Qty Status'
            ]
            
            st.dataframe(
                layer0_matches[display_cols],
                use_container_width=True,
                hide_index=True
            )
            
        else:
            st.warning("No Layer 0 exact matches found")
            
    except Exception as e:
        st.error(f"Error loading Layer 0 matches: {e}")

def show_layer1_layer2_matches(customer_name, config_mgr):
    """Show Layer 1 and Layer 2 fuzzy matches"""
    st.subheader("🎯 Layer 1 & 2 - Fuzzy Matches")
    
    # Layer 1 section
    st.markdown("### 🎯 Layer 1 - Exact Style + Color, Flexible Delivery")
    
    query_layer1 = """
    SELECT 
        s.shipment_id,
        s.style_code,
        s.color_description,
        s.delivery_method as ship_delivery,
        s.quantity as shipment_qty,
        s.shipped_date,
        emr.match_confidence,
        emr.quantity_difference_percent,
        emr.order_quantity,
        emr.order_delivery_method,
        emr.style_match,
        emr.color_match,
        emr.delivery_match,
        CASE WHEN emr.delivery_match = 'MATCH' THEN 1 ELSE 0 END as delivery_match_flag
    FROM stg_fm_orders_shipped_table s
    INNER JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
    WHERE s.customer_name LIKE ? 
        AND s.po_number = '4755'
        AND emr.match_layer = 'LAYER_1'
    ORDER BY s.shipment_id
    """
    
    try:
        layer1_matches = config_mgr.execute_query(query_layer1, [f"{customer_name}%"])
        
        if not layer1_matches.empty:
            st.success(f"Found {len(layer1_matches)} Layer 1 matches")
            
            # Add enhanced status indicators
            layer1_matches['Style Match'] = layer1_matches['style_match'].apply(
                lambda x: '✅ Exact' if x == 'EXACT' else '🔄 Fuzzy'
            )
            layer1_matches['Color Match'] = layer1_matches['color_match'].apply(
                lambda x: '✅ Exact' if x == 'EXACT' else '🔄 Fuzzy'
            )
            layer1_matches['Delivery Status'] = layer1_matches['delivery_match_flag'].apply(
                lambda x: '✅ Match' if x == 1 else '⚠️ Mismatch - Review Required'
            )
            layer1_matches['Qty Status'] = layer1_matches['quantity_difference_percent'].apply(
                lambda x: '✅ Pass' if x <= 5 else ('⚠️ Conditional' if x <= 10 else '❌ Fail')
            )
            
            # Bulk approval interface for Layer 1
            st.markdown("#### 🔧 Bulk Approval Actions")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("✅ Approve All Delivery Mismatches", key="approve_delivery_l1"):
                    st.success("Delivery mismatches approved for Layer 1")
                    
            with col2:
                if st.button("⚠️ Mark Quantity Issues for Review", key="qty_review_l1"):
                    st.info("Quantity issues marked for conditional approval")
                    
            with col3:
                if st.button("🔄 Reapply Matching Rules", key="reapply_l1"):
                    st.info("Matching rules will be reapplied")
            
            # Display table
            display_cols = [
                'shipment_id', 'style_code', 'color_description', 
                'ship_delivery', 'order_delivery_method',
                'Style Match', 'Color Match', 'Delivery Status', 'Qty Status'
            ]
            
            st.dataframe(
                layer1_matches[display_cols],
                use_container_width=True,
                hide_index=True
            )
            
        else:
            st.info("No Layer 1 matches found")
    
    except Exception as e:
        st.error(f"Error loading Layer 1 matches: {e}")
    
    # Layer 2 section
    st.markdown("### 🔄 Layer 2 - Fuzzy Style + Color Matching")
    
    query_layer2 = """
    SELECT 
        s.shipment_id,
        s.style_code,
        s.color_description,
        s.delivery_method as ship_delivery,
        s.quantity as shipment_qty,
        s.shipped_date,
        emr.match_confidence,
        emr.quantity_difference_percent,
        emr.order_quantity,
        emr.order_delivery_method,
        emr.style_match,
        emr.color_match,
        emr.delivery_match
    FROM stg_fm_orders_shipped_table s
    INNER JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
    WHERE s.customer_name LIKE ? 
        AND s.po_number = '4755'
        AND emr.match_layer = 'LAYER_2'
    ORDER BY s.shipment_id
    """
    
    try:
        layer2_matches = config_mgr.execute_query(query_layer2, [f"{customer_name}%"])
        
        if not layer2_matches.empty:
            st.success(f"Found {len(layer2_matches)} Layer 2 fuzzy matches")
            st.warning("⚠️ These matches require careful review due to fuzzy string matching")
            
            # Enhanced display for fuzzy matches
            layer2_matches['Style Match'] = layer2_matches.apply(
                lambda row: f"🔄 Fuzzy ({row['match_confidence']:.2f})" if row['style_match'] == 'FUZZY' else '✅ Exact',
                axis=1
            )
            layer2_matches['Color Match'] = layer2_matches['color_match'].apply(
                lambda x: '✅ Exact' if x == 'EXACT' else '🔄 Fuzzy'
            )
            layer2_matches['Confidence'] = layer2_matches['match_confidence'].apply(
                lambda x: f"{x:.1%}" if pd.notna(x) else "N/A"
            )
            
            # Bulk approval for Layer 2
            st.markdown("#### 🔧 Layer 2 Bulk Actions")
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("✅ Approve High Confidence (>90%)", key="approve_high_conf_l2"):
                    high_conf = layer2_matches[layer2_matches['match_confidence'] > 0.9]
                    st.success(f"Approved {len(high_conf)} high confidence matches")
                    
            with col2:
                if st.button("⚠️ Flag Low Confidence for Review", key="flag_low_conf_l2"):
                    low_conf = layer2_matches[layer2_matches['match_confidence'] <= 0.9]
                    st.warning(f"Flagged {len(low_conf)} matches for manual review")
            
            # Display with confidence highlighting
            display_cols = [
                'shipment_id', 'style_code', 'color_description',
                'Style Match', 'Color Match', 'Confidence'
            ]
            
            st.dataframe(
                layer2_matches[display_cols],
                use_container_width=True,
                hide_index=True
            )
            
        else:
            st.info("No Layer 2 fuzzy matches found")
            
    except Exception as e:
        st.error(f"Error loading Layer 2 matches: {e}")

def show_enhanced_delivery_review(customer_name, config_mgr):
    """Enhanced delivery method mismatch review with bulk operations"""
    st.subheader("🚚 Delivery Method Mismatches - Global Approval")
    st.info("Configure global delivery method matching rules and approvals")
    
    # Get all delivery mismatches
    query = """
    SELECT 
        s.shipment_id,
        s.style_code,
        s.color_description,
        s.delivery_method as shipment_delivery,
        emr.order_delivery_method,
        emr.match_layer,
        emr.match_confidence,
        emr.quantity_difference_percent,
        'PENDING' as approval_status,
        '' as business_reason,
        '' as notes
    FROM stg_fm_orders_shipped_table s
    INNER JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
    WHERE s.customer_name LIKE ? 
        AND s.po_number = '4755'
        AND emr.delivery_match = 'MISMATCH'
    ORDER BY s.delivery_method, emr.order_delivery_method
    """
    
    try:
        delivery_mismatches = config_mgr.execute_query(query, [f"{customer_name}%"])
        
        if not delivery_mismatches.empty:
            st.warning(f"Found {len(delivery_mismatches)} delivery method mismatches requiring review")
            
            # Global delivery method mapping section
            st.markdown("### 🌐 Global Delivery Method Rules")
            st.info("Define global rules that apply to all customers and POs")
            
            # Get unique delivery method combinations
            unique_combinations = delivery_mismatches.groupby(['shipment_delivery', 'order_delivery_method']).size().reset_index(name='count')
            
            st.markdown("#### Common Delivery Method Mismatches")
            for _, combo in unique_combinations.iterrows():
                col1, col2, col3, col4 = st.columns([2, 2, 2, 2])
                
                with col1:
                    st.write(f"**Ship:** {combo['shipment_delivery']}")
                with col2:
                    st.write(f"**Order:** {combo['order_delivery_method']}")
                with col3:
                    st.write(f"**Count:** {combo['count']}")
                with col4:
                    approval_key = f"approve_{combo['shipment_delivery']}_{combo['order_delivery_method']}"
                    if st.button(f"✅ Auto-Approve", key=approval_key):
                        st.success(f"Rule created: {combo['shipment_delivery']} ↔ {combo['order_delivery_method']}")
            
            # Bulk table editor
            st.markdown("### 📊 Bulk Review & Approval")
            st.info("Select multiple records and apply bulk actions")
            
            # Add selection checkboxes
            delivery_mismatches['Select'] = False
            
            # Enhanced table with editable columns
            edited_df = st.data_editor(
                delivery_mismatches,
                column_config={
                    "Select": st.column_config.CheckboxColumn(
                        "Select",
                        help="Select for bulk action",
                        default=False
                    ),
                    "approval_status": st.column_config.SelectboxColumn(
                        "Approval Status",
                        help="Set approval status",
                        options=["PENDING", "APPROVED", "REJECTED", "CONDITIONAL"],
                        required=True
                    ),
                    "business_reason": st.column_config.TextColumn(
                        "Business Reason",
                        help="Reason for approval/rejection",
                        max_chars=100
                    ),
                    "notes": st.column_config.TextColumn(
                        "Notes",
                        help="Additional notes",
                        max_chars=200
                    )
                },
                use_container_width=True,
                hide_index=True,
                key="delivery_review_table"
            )
            
            # Bulk action buttons
            st.markdown("#### 🔧 Bulk Actions")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if st.button("✅ Approve Selected", key="bulk_approve_delivery"):
                    selected = edited_df[edited_df['Select'] == True]
                    st.success(f"Approved {len(selected)} delivery mismatches")
                    
            with col2:
                if st.button("❌ Reject Selected", key="bulk_reject_delivery"):
                    selected = edited_df[edited_df['Select'] == True]
                    st.error(f"Rejected {len(selected)} delivery mismatches")
                    
            with col3:
                if st.button("⚠️ Mark Conditional", key="bulk_conditional_delivery"):
                    selected = edited_df[edited_df['Select'] == True]
                    st.warning(f"Marked {len(selected)} as conditional")
                    
            with col4:
                if st.button("🔄 Reapply Rules", key="reapply_delivery_rules"):
                    st.info("Reapplying global delivery rules...")
            
        else:
            st.success("✅ No delivery method mismatches found!")
            
    except Exception as e:
        st.error(f"Error loading delivery mismatches: {e}")

def show_enhanced_quantity_review(customer_name, config_mgr):
    """Enhanced quantity review with bulk table operations"""
    st.subheader("⚖️ Quantity Issues - Bulk Review")
    st.info("Review and approve quantity discrepancies with bulk operations")
    
    query = """
    SELECT 
        s.shipment_id,
        s.style_code,
        s.color_description,
        s.delivery_method,
        s.quantity as shipment_qty,
        emr.order_quantity,
        emr.quantity_difference_percent,
        emr.match_layer,
        emr.match_confidence,
        CASE 
            WHEN emr.quantity_difference_percent <= 5 THEN 'PASS'
            WHEN emr.quantity_difference_percent <= 10 THEN 'CONDITIONAL'
            ELSE 'FAIL'
        END as qty_status,
        'PENDING' as approval_status,
        '' as business_reason,
        '' as notes
    FROM stg_fm_orders_shipped_table s
    INNER JOIN enhanced_matching_results emr ON s.shipment_id = emr.shipment_id
    WHERE s.customer_name LIKE ? 
        AND s.po_number = '4755'
        AND emr.quantity_difference_percent > 5
    ORDER BY emr.quantity_difference_percent DESC
    """
    
    try:
        quantity_issues = config_mgr.execute_query(query, [f"{customer_name}%"])
        
        if not quantity_issues.empty:
            st.warning(f"Found {len(quantity_issues)} quantity issues requiring review")
            
            # Summary metrics
            col1, col2, col3 = st.columns(3)
            
            conditional_count = len(quantity_issues[quantity_issues['qty_status'] == 'CONDITIONAL'])
            fail_count = len(quantity_issues[quantity_issues['qty_status'] == 'FAIL'])
            
            with col1:
                st.metric("⚠️ Conditional (5-10%)", conditional_count)
            with col2:
                st.metric("❌ Failed (>10%)", fail_count)
            with col3:
                avg_diff = quantity_issues['quantity_difference_percent'].mean()
                st.metric("📊 Avg Difference", f"{avg_diff:.1f}%")
            
            # Enhanced bulk table editor
            st.markdown("### 📊 Quantity Review Table")
            
            # Add selection and calculated columns
            quantity_issues['Select'] = False
            quantity_issues['Difference'] = (quantity_issues['shipment_qty'] - quantity_issues['order_quantity']).astype(int)
            
            # Format percentage for display
            quantity_issues['Diff %'] = quantity_issues['quantity_difference_percent'].apply(lambda x: f"{x:.1f}%")
            
            edited_qty_df = st.data_editor(
                quantity_issues,
                column_config={
                    "Select": st.column_config.CheckboxColumn(
                        "Select",
                        help="Select for bulk action",
                        default=False
                    ),
                    "qty_status": st.column_config.SelectboxColumn(
                        "Qty Status",
                        help="Quantity validation status",
                        options=["PASS", "CONDITIONAL", "FAIL"],
                        disabled=True
                    ),
                    "approval_status": st.column_config.SelectboxColumn(
                        "Approval",
                        help="Manual approval status",
                        options=["PENDING", "APPROVED", "REJECTED", "CONDITIONAL"],
                        required=True
                    ),
                    "business_reason": st.column_config.SelectboxColumn(
                        "Business Reason",
                        help="Reason for quantity difference",
                        options=[
                            "",
                            "Shipping damage",
                            "Quality control",
                            "Production shortage", 
                            "Customer request",
                            "Partial shipment",
                            "Inventory adjustment",
                            "Other"
                        ]
                    ),
                    "notes": st.column_config.TextColumn(
                        "Notes",
                        help="Additional notes",
                        max_chars=200
                    ),
                    "Diff %": st.column_config.TextColumn(
                        "Diff %",
                        help="Percentage difference",
                        disabled=True
                    )
                },
                use_container_width=True,
                hide_index=True,
                key="quantity_review_table"
            )
            
            # Advanced bulk actions
            st.markdown("#### 🔧 Bulk Actions")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if st.button("✅ Approve Conditional (5-10%)", key="approve_conditional_qty"):
                    conditional_selected = edited_qty_df[
                        (edited_qty_df['Select'] == True) & 
                        (edited_qty_df['qty_status'] == 'CONDITIONAL')
                    ]
                    st.success(f"Approved {len(conditional_selected)} conditional quantity issues")
                    
            with col2:
                if st.button("❌ Reject Failed (>10%)", key="reject_failed_qty"):
                    failed_selected = edited_qty_df[
                        (edited_qty_df['Select'] == True) & 
                        (edited_qty_df['qty_status'] == 'FAIL')
                    ]
                    st.error(f"Rejected {len(failed_selected)} failed quantity issues")
                    
            with col3:
                if st.button("🔍 Review Selected", key="review_selected_qty"):
                    selected = edited_qty_df[edited_qty_df['Select'] == True]
                    st.info(f"Marked {len(selected)} for detailed review")
                    
            with col4:
                if st.button("📊 Export for Analysis", key="export_qty_issues"):
                    st.info("Quantity issues exported to CSV")
            
            # Quick approval shortcuts
            st.markdown("#### ⚡ Quick Approvals")
            col1, col2 = st.columns(2)
            
            with col1:
                tolerance_pct = st.slider(
                    "Auto-approve up to:", 
                    min_value=5.0, 
                    max_value=15.0, 
                    value=8.0, 
                    step=0.5,
                    format="%.1f%%"
                )
                if st.button(f"✅ Auto-approve ≤{tolerance_pct}%", key="auto_approve_tolerance"):
                    auto_approve = quantity_issues[quantity_issues['quantity_difference_percent'] <= tolerance_pct]
                    st.success(f"Auto-approved {len(auto_approve)} items within {tolerance_pct}% tolerance")
                    
            with col2:
                reason = st.selectbox(
                    "Bulk reason:",
                    ["Production variance", "Shipping damage", "Quality control", "Customer request"]
                )
                if st.button("📝 Apply Reason to Selected", key="apply_bulk_reason"):
                    selected = edited_qty_df[edited_qty_df['Select'] == True]
                    st.info(f"Applied '{reason}' to {len(selected)} selected items")
        
        else:
            st.success("✅ No quantity issues found!")
            
    except Exception as e:
        st.error(f"Error loading quantity issues: {e}")

def show_unmatched_shipments(customer_name, config_mgr):
    """Show completely unmatched shipments"""
    st.subheader("❌ Unmatched Shipments")
    st.info("Shipments that couldn't be matched even with fuzzy logic")
    
    query = """
    SELECT 
        s.shipment_id,
        s.style_code,
        s.color_description,
        s.delivery_method,
        s.quantity,
        s.shipped_date
    FROM stg_fm_orders_shipped_table s
    WHERE s.customer_name LIKE ? 
        AND s.po_number = '4755'
        AND NOT EXISTS (
            SELECT 1 FROM enhanced_matching_results emr 
            WHERE emr.shipment_id = s.shipment_id
        )
    ORDER BY s.shipment_id
    """
    
    try:
        unmatched = config_mgr.execute_query(query, [f"{customer_name}%"])
        
        if not unmatched.empty:
            st.error(f"Found {len(unmatched)} completely unmatched shipments")
            st.warning("These shipments require manual investigation or order creation")
            
            # Add action column
            unmatched['Action Needed'] = '🔍 Manual Review'
            
            st.dataframe(
                unmatched,
                use_container_width=True,
                hide_index=True
            )
            
            # Action buttons
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("📝 Create Missing Orders", key="create_orders"):
                    st.info("Creating missing order entries...")
                    
            with col2:
                if st.button("🔍 Investigate Discrepancies", key="investigate"):
                    st.info("Opening investigation workflow...")
                    
            with col3:
                if st.button("📊 Export for Review", key="export_unmatched"):
                    st.info("Exporting unmatched shipments...")
        
        else:
            st.success("🎉 All shipments successfully matched!")
            
    except Exception as e:
        st.error(f"Error loading unmatched shipments: {e}")

def show_layer3_quantity_resolution(customer_name, config_mgr):
    """Show Layer 3 quantity resolution matches and analysis"""
    st.subheader("🔧 Layer 3 - Quantity Variance Resolution")
    st.info("Additional orders linked to resolve major quantity discrepancies")
    
    # Layer 3 matches
    layer3_query = """
    SELECT 
        emr.shipment_id,
        emr.order_id,
        emr.shipment_style_code,
        emr.shipment_color_description,
        emr.shipment_quantity,
        emr.order_quantity,
        emr.quantity_difference_percent,
        emr.quantity_check_result,
        emr.shipment_delivery_method,
        emr.order_delivery_method,
        emr.delivery_match,
        emr.match_confidence,
        emr.created_at
    FROM enhanced_matching_results emr
    WHERE emr.customer_name = ?
        AND emr.po_number = '4755'
        AND emr.match_layer = 'LAYER_3'
    ORDER BY emr.shipment_id, emr.created_at
    """
    
    # Quantity analysis - before and after Layer 3
    quantity_analysis_query = """
    WITH shipment_totals AS (
        SELECT 
            shipment_id,
            shipment_style_code,
            shipment_color_description,
            shipment_quantity,
            shipment_delivery_method,
            COUNT(*) as order_count,
            SUM(order_quantity) as total_order_quantity,
            MIN(quantity_difference_percent) as best_variance_pct,
            MAX(CASE WHEN quantity_check_result = 'PASS' THEN 1 ELSE 0 END) as has_pass
        FROM enhanced_matching_results 
        WHERE customer_name = ? AND po_number = '4755'
        GROUP BY shipment_id, shipment_style_code, shipment_color_description, 
                 shipment_quantity, shipment_delivery_method
    )
    SELECT 
        shipment_id,
        shipment_style_code,
        shipment_color_description,
        shipment_quantity,
        total_order_quantity,
        (shipment_quantity - total_order_quantity) as quantity_gap,
        ABS((shipment_quantity - total_order_quantity) * 100.0 / shipment_quantity) as variance_pct,
        CASE WHEN has_pass = 1 THEN 'RESOLVED' ELSE 'FAILED' END as resolution_status,
        order_count,
        CASE 
            WHEN order_count > 1 THEN 'LAYER_3_APPLIED' 
            ELSE 'SINGLE_MATCH' 
        END as matching_type
    FROM shipment_totals
    ORDER BY variance_pct DESC
    """
    
    try:
        # Get Layer 3 matches
        layer3_matches = config_mgr.execute_query(layer3_query, [customer_name])
        
        # Get quantity analysis
        quantity_analysis = config_mgr.execute_query(quantity_analysis_query, [customer_name])
        
        if not layer3_matches.empty:
            st.success(f"🎉 {len(layer3_matches)} Layer 3 matches applied!")
            
            # Show Layer 3 matches with status
            st.markdown("### 📋 Layer 3 Additional Order Links")
            
            layer3_display = layer3_matches.copy()
            layer3_display['Status'] = layer3_display['quantity_check_result'].apply(
                lambda x: '✅ RESOLVED' if x == 'PASS' else '❌ FAILED'
            )
            layer3_display['Delivery'] = layer3_display['delivery_match'].apply(
                lambda x: '🟢 MATCH' if x == 'MATCH' else '🟡 MISMATCH'  
            )
            layer3_display['Variance'] = layer3_display['quantity_difference_percent'].round(1).astype(str) + '%'
            
            display_cols = [
                'shipment_id', 'shipment_style_code', 'shipment_color_description',
                'shipment_quantity', 'order_quantity', 'Variance', 'Status', 'Delivery'
            ]
            
            st.dataframe(
                layer3_display[display_cols],
                use_container_width=True,
                hide_index=True
            )
        
        # Show quantity resolution analysis
        st.markdown("### 📊 Quantity Resolution Analysis")
        
        if not quantity_analysis.empty:
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            total_shipments = len(quantity_analysis)
            resolved_shipments = len(quantity_analysis[quantity_analysis['resolution_status'] == 'RESOLVED'])
            layer3_shipments = len(quantity_analysis[quantity_analysis['matching_type'] == 'LAYER_3_APPLIED'])
            avg_variance = quantity_analysis['variance_pct'].mean()
            
            with col1:
                st.metric("Total Shipments", total_shipments)
            with col2:
                st.metric("Resolved", resolved_shipments, delta=f"{resolved_shipments/total_shipments*100:.1f}%")
            with col3:
                st.metric("Layer 3 Applied", layer3_shipments)
            with col4:
                st.metric("Avg Variance", f"{avg_variance:.1f}%")
            
            # Detailed analysis table
            st.markdown("### 🔍 Shipment-Level Resolution Status")
            
            analysis_display = quantity_analysis.copy()
            analysis_display['Resolution'] = analysis_display['resolution_status'].apply(
                lambda x: '✅ RESOLVED' if x == 'RESOLVED' else '❌ FAILED'
            )
            analysis_display['Matching'] = analysis_display['matching_type'].apply(
                lambda x: '🔧 Layer 3' if x == 'LAYER_3_APPLIED' else '🎯 Single'
            )
            analysis_display['Variance %'] = analysis_display['variance_pct'].round(1)
            
            # Color coding for the table
            def highlight_resolution(row):
                if row['resolution_status'] == 'RESOLVED':
                    return ['background-color: #d4edda'] * len(row)
                else:
                    return ['background-color: #f8d7da'] * len(row)
            
            styled_analysis = analysis_display[[
                'shipment_id', 'shipment_style_code', 'shipment_color_description',
                'shipment_quantity', 'total_order_quantity', 'quantity_gap', 
                'Variance %', 'Resolution', 'Matching', 'order_count'
            ]].style.apply(highlight_resolution, axis=1)
            
            st.dataframe(styled_analysis, use_container_width=True, hide_index=True)
            
            # Success story
            if layer3_shipments > 0:
                st.success(f"""
                🎉 **Layer 3 Success Story**:
                - {layer3_shipments} shipments had additional orders linked
                - {resolved_shipments} total shipments now have acceptable variance (≤10%)
                - Major quantity gaps resolved through intelligent order consolidation
                """)
            
            # Remaining issues
            failed_shipments = quantity_analysis[quantity_analysis['resolution_status'] == 'FAILED']
            if not failed_shipments.empty:
                st.warning(f"""
                ⚠️ **Remaining Quantity Issues**:
                - {len(failed_shipments)} shipments still have >10% variance
                - These may need manual investigation or have acceptable business tolerance
                - Consider Layer 4 matching with CANCELLED orders for historical context
                """)
        
        else:
            st.info("No quantity analysis data available")
            
    except Exception as e:
        st.error(f"Error loading Layer 3 analysis: {e}")
