import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pymysql
import plotly.graph_objects as go
import plotly.express as px
import warnings
warnings.filterwarnings('ignore')

# Set page config
st.set_page_config(
    page_title="Business Development Performance Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'transactions' not in st.session_state:
    st.session_state.transactions = None
if 'onboarding' not in st.session_state:
    st.session_state.onboarding = None
if 'period_data' not in st.session_state:
    st.session_state.period_data = None
if 'date_range_set' not in st.session_state:
    st.session_state.date_range_set = False
if 'start_date' not in st.session_state:
    st.session_state.start_date = datetime(2025, 10, 1)
if 'end_date' not in st.session_state:
    st.session_state.end_date = min(datetime(2026, 1, 14), datetime.now())

class DatabaseManager:
    def __init__(self):
        self.connection = None
        
    def connect(self):
        """Connect to MySQL database"""
        try:
            self.connection = pymysql.connect(
                host='db4free.net',
                database='bdp_report',
                user='lamin_d_kinteh',
                password='Lamin@123',
                port=3306,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10
            )
            return True
        except Exception as e:
            st.error(f"Database connection error: {str(e)}")
            return False
    
    def disconnect(self):
        """Disconnect from database"""
        if self.connection:
            self.connection.close()
    
    def execute_query(self, query, params=None):
        """Execute SQL query and return DataFrame"""
        try:
            with self.connection.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                result = cursor.fetchall()
                
                if result:
                    return pd.DataFrame(result)
                else:
                    return pd.DataFrame()
        except Exception as e:
            st.error(f"Query execution error: {str(e)}")
            return pd.DataFrame()

class PerformanceDashboard:
    def __init__(self):
        self.db = DatabaseManager()
        self.today = datetime.now()
        
        # Define product categories based on your transaction data
        self.product_categories = {
            'P2P (Internal Wallet Transfer)': ['Internal Wallet Transfer'],
            'Cash-In': ['Deposit'],
            'Cash-Out': ['Scan To Withdraw Agent', 'Scan To Withdraw Customer', 'OTP Withdrawal'],
            'Disbursement': ['Disbursement'],
            'Cash Power': ['Nawec Cashpower'],
            'E-Ticketing': ['Ticket'],
            'Bank Transfers': ['BANK_TO_WALLET_TRANSFER', 'WALLET_TO_BANK_TRANSFER'],
            'Airtime': ['Africell', 'Qcell', 'Comium'],
            'International': ['International Remittance'],
            'Money Transfer': ['Scan to Send Money', 'Wallet to Non Wallet Transfer'],
            'Add Money': ['Add Money'],
            'APS PAY': ['APS PAY']
        }
        
        self.all_products = []
        for category, products in self.product_categories.items():
            self.all_products.extend(products)
        
        # Define service names for Airtime Topup
        self.services = ['Airtime Topup']
    
    def set_date_range(self, start_date, end_date):
        """Set the date range for analysis"""
        self.start_date_overall = start_date
        self.end_date_overall = end_date
        st.session_state.start_date = start_date
        st.session_state.end_date = end_date
        st.session_state.date_range_set = True
    
    def load_data_from_db(self, start_date, end_date):
        """Load data from MySQL database for given date range"""
        # Validate date range
        if start_date > end_date:
            st.error("❌ Error: Start date cannot be after end date!")
            return False
            
        self.set_date_range(start_date, end_date)
        
        if not self.db.connect():
            return False
        
        try:
            # Load transaction data - CORRECTED: Using your actual column names
            transaction_query = """
                SELECT 
                    id,
                    user_identifier,
                    transaction_id,
                    sub_transaction_id,
                    entity_name,
                    full_name,
                    created_by,
                    status,
                    internal_status,
                    service_name,
                    product_name,
                    transaction_type,
                    amount,
                    before_balance,
                    after_balance,
                    ucp_name,
                    wallet_name,
                    pouch_name,
                    reference,
                    error_code,
                    error_message,
                    vendor_transaction_id,
                    vendor_response_code,
                    vendor_message,
                    slug,
                    remarks,
                    created_at,
                    business_hierarchy,
                    parent_user_identifier,
                    parent_full_name
                FROM Transaction 
                WHERE created_at IS NOT NULL 
                AND created_at >= %s 
                AND created_at <= %s
                ORDER BY created_at DESC
            """
            
            transactions = self.db.execute_query(
                transaction_query, 
                (self.start_date_overall, self.end_date_overall)
            )
            
            if transactions.empty:
                st.warning("No transaction data found for the selected period")
                self.db.disconnect()
                return False
            
            # Debug: Show column names
            st.sidebar.write(f"📊 Loaded {len(transactions):,} transactions")
            st.sidebar.write(f"📅 Date range: {start_date} to {end_date}")
            
            # Clean column names - keep original names but ensure consistency
            transactions.columns = [col.strip().replace(' ', '_').lower() for col in transactions.columns]
            
            # Parse dates
            if 'created_at' in transactions.columns:
                transactions['created_at'] = pd.to_datetime(transactions['created_at'], errors='coerce')
                # Remove invalid dates
                valid_dates = transactions['created_at'].notna()
                transactions = transactions[valid_dates].copy()
                st.sidebar.write(f"✅ Valid transaction dates: {len(transactions):,}")
            
            # Clean amount
            if 'amount' in transactions.columns:
                transactions['amount'] = pd.to_numeric(
                    transactions['amount'].astype(str).str.replace('[^0-9.-]', '', regex=True),
                    errors='coerce'
                )
            
            # Clean text columns - CORRECTED: Use actual column names from your data
            text_columns = ['user_identifier', 'product_name', 'entity_name', 'transaction_type', 
                          'ucp_name', 'service_name', 'status', 'sub_transaction_id', 'full_name']
            for col in text_columns:
                if col in transactions.columns:
                    transactions[col] = transactions[col].astype(str).str.strip()
            
            # Load onboarding data - CORRECTED: Using your actual column names
            onboarding_query = """
                SELECT 
                    account_id,
                    full_name,
                    mobile,
                    email,
                    region,
                    district,
                    town_village,
                    business_name,
                    kyc_status,
                    registration_date,
                    updated_at,
                    proof_of_id,
                    identification_number,
                    customer_referrer_code,
                    customer_referrer_mobile,
                    referrer_entity,
                    entity,
                    bank,
                    bank_account_name,
                    bank_account_number,
                    status
                FROM Onboarding 
                WHERE registration_date IS NOT NULL 
                AND registration_date >= %s 
                AND registration_date <= %s
            """
            
            onboarding = self.db.execute_query(
                onboarding_query,
                (self.start_date_overall, self.end_date_overall)
            )
            
            if not onboarding.empty:
                # Clean column names
                onboarding.columns = [col.strip().replace(' ', '_').lower() for col in onboarding.columns]
                
                # Parse dates
                if 'registration_date' in onboarding.columns:
                    onboarding['registration_date'] = pd.to_datetime(onboarding['registration_date'], errors='coerce')
                    valid_reg_dates = onboarding['registration_date'].notna()
                    onboarding = onboarding[valid_reg_dates].copy()
                
                # Create User Identifier for merging
                if 'mobile' in onboarding.columns:
                    onboarding['user_identifier'] = onboarding['mobile'].astype(str).str.strip()
                elif 'account_id' in onboarding.columns:
                    onboarding['user_identifier'] = onboarding['account_id'].astype(str).str.strip()
                
                # Clean text columns
                text_cols = ['status', 'entity', 'kyc_status']
                for col in text_cols:
                    if col in onboarding.columns:
                        onboarding[col] = onboarding[col].astype(str).str.strip()
                
                st.sidebar.write(f"👥 Loaded {len(onboarding):,} onboarding records")
            
            self.db.disconnect()
            
            # Store in session state
            st.session_state.transactions = transactions
            st.session_state.onboarding = onboarding if not onboarding.empty else pd.DataFrame()
            st.session_state.data_loaded = True
            
            # Show data summary
            st.sidebar.success("✅ Data loaded successfully!")
            
            return True
            
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            import traceback
            traceback.print_exc()
            if self.db.connection:
                self.db.disconnect()
            return False
    
    def get_report_periods(self, period_type='monthly'):
        """Get report periods based on selected period type"""
        if not hasattr(self, 'start_date_overall') or self.start_date_overall is None:
            return []
            
        periods = []
        
        if period_type == 'monthly':
            # Generate monthly periods
            current_date = self.start_date_overall
            while current_date <= self.end_date_overall:
                month_end = (current_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                month_end = min(month_end, self.end_date_overall)
                
                period_name = current_date.strftime('%B %Y')
                periods.append((period_name, current_date, month_end, 'monthly'))
                
                current_date = month_end + timedelta(days=1)
        
        elif period_type == 'weekly':
            # Generate weekly periods
            current_date = self.start_date_overall
            while current_date <= self.end_date_overall:
                week_end = current_date + timedelta(days=6)
                week_end = min(week_end, self.end_date_overall)
                
                period_name = f"Week {(current_date.day - 1) // 7 + 1}, {current_date.strftime('%B %Y')}"
                periods.append((period_name, current_date, week_end, 'weekly'))
                current_date = week_end + timedelta(days=1)
        
        elif period_type == 'rolling':
            # Generate rolling 7-day periods
            start_date = self.start_date_overall
            while start_date <= self.end_date_overall:
                period_end = min(start_date + timedelta(days=6), self.end_date_overall)
                if period_end > start_date:
                    period_name = f"{start_date.strftime('%b %d')} - {period_end.strftime('%b %d')}"
                    periods.append((period_name, start_date, period_end, 'rolling'))
                start_date = start_date + timedelta(days=7)
        
        return periods
    
    def filter_by_date_range(self, df, date_col, start_date, end_date):
        """Filter dataframe by date range"""
        if df is None or df.empty or date_col not in df.columns:
            return pd.DataFrame()
        
        mask = (df[date_col] >= pd.Timestamp(start_date)) & (df[date_col] <= pd.Timestamp(end_date))
        return df[mask].copy()
    
    def get_new_registered_customers_segmented(self, start_date, end_date):
        """Get new registered customers segmented by Status - CORRECTED"""
        period_onboarding = self.filter_by_date_range(
            st.session_state.onboarding, 'registration_date', start_date, end_date
        )
        
        # Initialize counts - CORRECTED: Using your actual status values
        segmented_counts = {'Active': 0, 'Registered': 0, 'Temporary': 0, 'Total': 0}
        customer_lists = {'Active': [], 'Registered': [], 'Temporary': [], 'Total': []}
        
        if not period_onboarding.empty and 'entity' in period_onboarding.columns and 'status' in period_onboarding.columns:
            # Clean status values - handle case variations
            period_onboarding['status_clean'] = period_onboarding['status'].astype(str).str.strip()
            
            # Map status values to categories - CORRECTED for your data
            status_mapping = {
                'Active': ['Active', 'ACTIVE', 'active'],
                'Registered': ['Registered', 'REGISTERED', 'registered'],
                'Temporary': ['TemporaryRegister', 'TEMPORARYREGISTER', 'Temporary', 'temporary']
            }
            
            # Filter customers (entity = Customer)
            customer_mask = period_onboarding['entity'].astype(str).str.contains('Customer', case=False, na=False)
            customers = period_onboarding[customer_mask].copy()
            
            if not customers.empty:
                # Count by status category
                for status_category, status_values in status_mapping.items():
                    mask = customers['status_clean'].isin(status_values)
                    status_customers = customers[mask]
                    
                    if 'user_identifier' in status_customers.columns:
                        segmented_counts[status_category] = status_customers['user_identifier'].nunique()
                        customer_lists[status_category] = status_customers['user_identifier'].dropna().unique().tolist()
                
                # Total (all statuses)
                if 'user_identifier' in customers.columns:
                    segmented_counts['Total'] = customers['user_identifier'].nunique()
                    customer_lists['Total'] = customers['user_identifier'].dropna().unique().tolist()
        
        return segmented_counts, customer_lists
    
    def get_active_customers_all(self, start_date, end_date, period_type):
        """Get ALL active customers (customers with at least 1 successful transaction) - CORRECTED"""
        period_transactions = self.filter_by_date_range(
            st.session_state.transactions, 'created_at', start_date, end_date
        )
        
        if period_transactions.empty:
            return [], 0
        
        # Debug: Check available columns
        available_cols = period_transactions.columns.tolist()
        
        # Filter successful customer transactions - CORRECTED for your data
        # First, identify customer transactions
        customer_mask = period_transactions['entity_name'].astype(str).str.contains('Customer', case=False, na=False)
        
        # Check status values
        unique_statuses = period_transactions['status'].dropna().unique() if 'status' in period_transactions.columns else []
        
        # Define success statuses - CORRECTED for your data
        success_keywords = ['SUCCESS', 'COMPLETED', 'APPROVED', 'SUCCESSFUL']
        
        # Find matching statuses
        success_statuses = []
        for status in unique_statuses:
            status_str = str(status).upper()
            for keyword in success_keywords:
                if keyword in status_str:
                    success_statuses.append(status)
                    break
        
        # If no exact matches, use any status that's not clearly an error
        if not success_statuses:
            error_keywords = ['FAILED', 'ERROR', 'DECLINED', 'REJECTED', 'CANCELLED']
            for status in unique_statuses:
                status_str = str(status).upper()
                is_error = any(keyword in status_str for keyword in error_keywords)
                if not is_error and len(status_str) > 0:
                    success_statuses.append(status)
        
        # If still no statuses, assume all non-null statuses are successful
        if not success_statuses and len(unique_statuses) > 0:
            success_statuses = list(unique_statuses)
        
        # Filter customer transactions with success status
        if success_statuses:
            customer_transactions = period_transactions[
                customer_mask & 
                period_transactions['status'].isin(success_statuses)
            ]
        else:
            # If no status filter, just use customer transactions
            customer_transactions = period_transactions[customer_mask]
        
        if customer_transactions.empty:
            return [], 0
        
        # Get unique active customers
        if 'user_identifier' in customer_transactions.columns:
            active_customers = customer_transactions['user_identifier'].dropna().unique().tolist()
            return active_customers, len(active_customers)
        elif 'full_name' in customer_transactions.columns:
            active_customers = customer_transactions['full_name'].dropna().unique().tolist()
            return active_customers, len(active_customers)
        else:
            return [], 0
    
    def calculate_executive_snapshot(self, start_date, end_date, period_type, prev_start=None, prev_end=None):
        """Calculate Executive Snapshot metrics - CORRECTED"""
        metrics = {}
        
        # Get new registered customers SEGMENTED BY STATUS
        segmented_counts, segmented_lists = self.get_new_registered_customers_segmented(start_date, end_date)
        
        metrics['new_customers_active'] = segmented_counts['Active']
        metrics['new_customers_registered'] = segmented_counts['Registered']
        metrics['new_customers_temporary'] = segmented_counts['Temporary']
        metrics['new_customers_total'] = segmented_counts['Total']
        
        # Get ALL active customers (customers with at least 1 successful transaction)
        active_customers_all, active_count_all = self.get_active_customers_all(start_date, end_date, period_type)
        metrics['active_customers_all'] = active_count_all
        
        # Weekly Active Users (WAU) from new registered customers - BY STATUS
        wau_by_status = {'Active': 0, 'Registered': 0, 'Temporary': 0, 'Total': 0}
        
        # Get transactions for the period
        period_transactions = self.filter_by_date_range(
            st.session_state.transactions, 'created_at', start_date, end_date
        )
        
        if not period_transactions.empty:
            # Filter successful transactions
            customer_mask = period_transactions['entity_name'].astype(str).str.contains('Customer', case=False, na=False)
            
            # Define success statuses
            success_keywords = ['SUCCESS', 'COMPLETED', 'APPROVED', 'SUCCESSFUL']
            success_mask = period_transactions['status'].astype(str).str.upper().str.contains('|'.join(success_keywords), na=False)
            
            success_transactions = period_transactions[customer_mask & success_mask]
            
            for status in ['Active', 'Registered', 'Temporary']:
                status_customers = segmented_lists[status]
                if status_customers and not success_transactions.empty:
                    # Find status customers who transacted
                    if 'user_identifier' in success_transactions.columns:
                        transacting_customers = success_transactions[
                            success_transactions['user_identifier'].isin(status_customers)
                        ]['user_identifier'].unique()
                        wau_by_status[status] = len(transacting_customers)
        
        metrics['wau_active'] = wau_by_status['Active']
        metrics['wau_registered'] = wau_by_status['Registered']
        metrics['wau_temporary'] = wau_by_status['Temporary']
        metrics['wau_total'] = sum(wau_by_status.values())
        
        # Net Customer Growth
        if prev_start and prev_end:
            prev_segmented_counts, _ = self.get_new_registered_customers_segmented(prev_start, prev_end)
            
            if prev_segmented_counts['Total'] > 0:
                net_growth = ((segmented_counts['Total'] - prev_segmented_counts['Total']) / prev_segmented_counts['Total']) * 100
            else:
                net_growth = 0 if segmented_counts['Total'] > 0 else None
        else:
            net_growth = None
        
        metrics['net_growth_pct'] = net_growth
        
        # Top and Lowest Performing Products (by transaction count)
        if not period_transactions.empty and 'product_name' in period_transactions.columns:
            # Filter to customer transactions
            customer_mask = period_transactions['entity_name'].astype(str).str.contains('Customer', case=False, na=False)
            success_keywords = ['SUCCESS', 'COMPLETED', 'APPROVED', 'SUCCESSFUL']
            success_mask = period_transactions['status'].astype(str).str.upper().str.contains('|'.join(success_keywords), na=False)
            
            customer_transactions = period_transactions[customer_mask & success_mask]
            
            if not customer_transactions.empty and 'product_name' in customer_transactions.columns:
                # Clean product names
                customer_transactions['product_name_clean'] = customer_transactions['product_name'].astype(str).str.strip()
                
                # Count transactions per product
                product_counts = customer_transactions['product_name_clean'].value_counts()
                
                if not product_counts.empty:
                    # Get top performing product
                    top_product = product_counts.index[0]
                    top_product_count = int(product_counts.iloc[0])
                    
                    # Get lowest performing product (with at least 1 transaction)
                    active_products = product_counts[product_counts > 0]
                    if not active_products.empty:
                        low_product = active_products.index[-1]
                        low_product_count = int(active_products.iloc[-1])
                    else:
                        low_product = 'N/A'
                        low_product_count = 0
                    
                    metrics['top_product'] = top_product
                    metrics['top_product_count'] = top_product_count
                    metrics['low_product'] = low_product
                    metrics['low_product_count'] = low_product_count
                else:
                    metrics['top_product'] = 'N/A'
                    metrics['low_product'] = 'N/A'
            else:
                metrics['top_product'] = 'N/A'
                metrics['low_product'] = 'N/A'
        else:
            metrics['top_product'] = 'N/A'
            metrics['low_product'] = 'N/A'
        
        return metrics
    
    def calculate_customer_acquisition(self, start_date, end_date, prev_start=None, prev_end=None):
        """Calculate Customer Acquisition metrics - CORRECTED"""
        metrics = {}
        
        # Filter onboarding for the period
        period_onboarding = self.filter_by_date_range(
            st.session_state.onboarding, 'registration_date', start_date, end_date
        )
        
        # Get segmented customer counts
        segmented_counts, segmented_lists = self.get_new_registered_customers_segmented(start_date, end_date)
        
        # New Registrations by Status - CORRECTED naming
        metrics['new_registrations_active'] = segmented_counts['Active']
        metrics['new_registrations_registered'] = segmented_counts['Registered']
        metrics['new_registrations_temporary'] = segmented_counts['Temporary']
        metrics['new_registrations_total'] = segmented_counts['Total']
        
        # KYC Completed (Status = Active and KYC Status = Verified)
        kyc_completed = 0
        if not period_onboarding.empty and 'kyc_status' in period_onboarding.columns:
            # Clean KYC status
            period_onboarding['kyc_status_clean'] = period_onboarding['kyc_status'].astype(str).str.strip().str.upper()
            
            # Count verified KYC
            verified_mask = period_onboarding['kyc_status_clean'].str.contains('VERIFIED|COMPLETED|APPROVED', na=False)
            kyc_completed = period_onboarding[verified_mask]['user_identifier'].nunique() if 'user_identifier' in period_onboarding.columns else 0
        
        metrics['kyc_completed'] = kyc_completed
        
        # Calculate KYC Rate
        if metrics['new_registrations_total'] > 0:
            metrics['kyc_rate'] = (metrics['kyc_completed'] / metrics['new_registrations_total']) * 100
        else:
            metrics['kyc_rate'] = 0
        
        # First-Time Transactors (FTT) - New registered customers who transacted
        new_customers_total = segmented_lists['Total']
        ftt_count = 0
        
        if new_customers_total:
            # Get transactions for the period
            period_transactions = self.filter_by_date_range(
                st.session_state.transactions, 'created_at', start_date, end_date
            )
            
            if not period_transactions.empty:
                # Filter successful customer transactions
                customer_mask = period_transactions['entity_name'].astype(str).str.contains('Customer', case=False, na=False)
                success_keywords = ['SUCCESS', 'COMPLETED', 'APPROVED', 'SUCCESSFUL']
                success_mask = period_transactions['status'].astype(str).str.upper().str.contains('|'.join(success_keywords), na=False)
                
                customer_transactions = period_transactions[customer_mask & success_mask]
                
                if not customer_transactions.empty and 'user_identifier' in customer_transactions.columns:
                    # Find new customers who transacted
                    transacting_new_customers = customer_transactions[
                        customer_transactions['user_identifier'].isin(new_customers_total)
                    ]['user_identifier'].unique()
                    
                    ftt_count = len(transacting_new_customers)
        
        metrics['ftt'] = ftt_count
        
        # FTT Rate
        if metrics['new_registrations_total'] > 0:
            metrics['ftt_rate'] = (metrics['ftt'] / metrics['new_registrations_total']) * 100
        else:
            metrics['ftt_rate'] = 0
        
        # Activation Rate and Reactivated Users - simplified for now
        metrics['activation_rate'] = 0
        metrics['reactivated_count'] = 0
        
        return metrics
    
    def calculate_product_usage_performance(self, start_date, end_date, period_type, prev_start=None, prev_end=None):
        """Calculate Product Usage Performance metrics"""
        period_transactions = self.filter_by_date_range(
            st.session_state.transactions, 'created_at', start_date, end_date
        )
        
        product_metrics = {}
        
        if period_transactions.empty:
            return product_metrics
        
        # Filter successful customer transactions
        customer_mask = period_transactions['entity_name'].astype(str).str.contains('Customer', case=False, na=False)
        success_keywords = ['SUCCESS', 'COMPLETED', 'APPROVED', 'SUCCESSFUL']
        success_mask = period_transactions['status'].astype(str).str.upper().str.contains('|'.join(success_keywords), na=False)
        
        customer_transactions = period_transactions[customer_mask & success_mask]
        
        if customer_transactions.empty or 'product_name' not in customer_transactions.columns:
            return product_metrics
        
        # Clean product names
        customer_transactions['product_name_clean'] = customer_transactions['product_name'].astype(str).str.strip()
        
        # Group by product
        for product_name, product_group in customer_transactions.groupby('product_name_clean'):
            if product_name and str(product_name).lower() != 'nan':
                # Count metrics
                total_transactions = len(product_group)
                
                # Count unique users
                if 'user_identifier' in product_group.columns:
                    unique_users = product_group['user_identifier'].nunique()
                else:
                    unique_users = 0
                
                # Calculate amount
                total_amount = 0
                if 'amount' in product_group.columns:
                    total_amount = product_group['amount'].sum()
                
                # Determine category
                category = 'Other'
                for cat, products in self.product_categories.items():
                    if product_name in products:
                        category = cat
                        break
                
                product_metrics[product_name] = {
                    'category': category,
                    'total_transactions': int(total_transactions),
                    'total_amount': float(total_amount),
                    'unique_users': int(unique_users),
                    'avg_amount': float(total_amount / total_transactions) if total_transactions > 0 else 0
                }
        
        return product_metrics
    
    def calculate_product_penetration(self, start_date, end_date, period_type):
        """Calculate Product Penetration metrics"""
        # Get active customers
        active_customers_all, wau_all = self.get_active_customers_all(start_date, end_date, period_type)
        
        # Get product usage
        product_usage = self.calculate_product_usage_performance(start_date, end_date, period_type)
        
        penetration_metrics = {}
        
        for product_name, metrics in product_usage.items():
            unique_users = metrics.get('unique_users', 0)
            
            # Calculate penetration percentage
            penetration_pct = (unique_users / wau_all * 100) if wau_all > 0 else 0
            
            # Get new customers for FTT calculation
            _, segmented_lists = self.get_new_registered_customers_segmented(start_date, end_date)
            new_customers_total = segmented_lists['Total']
            
            # FTT users (simplified - would need transaction data per product)
            ftt_users_count = 0
            
            penetration_metrics[product_name] = {
                'penetration_pct': penetration_pct,
                'ftt_users_count': ftt_users_count,
                'repeat_users_count': 0,  # Simplified
                'drop_off_rate': 0,  # Simplified
                'total_product_users': unique_users,
                'active_product_users': unique_users  # All users are active in this period
            }
        
        return penetration_metrics, wau_all
    
    def calculate_customer_activity_engagement(self, start_date, end_date, period_type):
        """Calculate Customer Activity & Engagement metrics - CORRECTED"""
        period_transactions = self.filter_by_date_range(
            st.session_state.transactions, 'created_at', start_date, end_date
        )
        
        metrics = {}
        
        if period_transactions.empty:
            metrics = {
                'wau': 0,
                'avg_transactions_per_user': 0,
                'avg_products_per_user': 0,
                'dormant_users': 0,
                'reactivated_users': 0,
                'total_transactions': 0
            }
            return metrics
        
        # Filter successful customer transactions
        customer_mask = period_transactions['entity_name'].astype(str).str.contains('Customer', case=False, na=False)
        success_keywords = ['SUCCESS', 'COMPLETED', 'APPROVED', 'SUCCESSFUL']
        success_mask = period_transactions['status'].astype(str).str.upper().str.contains('|'.join(success_keywords), na=False)
        
        customer_transactions = period_transactions[customer_mask & success_mask]
        
        if customer_transactions.empty:
            metrics = {
                'wau': 0,
                'avg_transactions_per_user': 0,
                'avg_products_per_user': 0,
                'dormant_users': 0,
                'reactivated_users': 0,
                'total_transactions': 0
            }
            return metrics
        
        # Total transactions
        total_transactions = len(customer_transactions)
        
        # Weekly Active Users (WAU) - customers with at least 1 transaction
        if 'user_identifier' in customer_transactions.columns:
            wau_customers = customer_transactions['user_identifier'].dropna().unique()
            wau = len(wau_customers)
            
            # Average Transactions per Active User
            if wau > 0:
                transactions_per_user = customer_transactions.groupby('user_identifier').size()
                avg_transactions_per_user = transactions_per_user.mean()
            else:
                avg_transactions_per_user = 0
            
            # Average Products per User
            if wau > 0:
                products_per_user = customer_transactions.groupby('user_identifier')['product_name'].nunique()
                avg_products_per_user = products_per_user.mean()
            else:
                avg_products_per_user = 0
        else:
            wau = 0
            avg_transactions_per_user = 0
            avg_products_per_user = 0
        
        # Simplified dormant and reactivated users
        dormant_users = 0
        reactivated_users = 0
        
        metrics.update({
            'wau': wau,
            'total_transactions': total_transactions,
            'avg_transactions_per_user': avg_transactions_per_user,
            'avg_products_per_user': avg_products_per_user,
            'dormant_users': dormant_users,
            'reactivated_users': reactivated_users
        })
        
        return metrics
    
    def generate_period_report(self, period_type='monthly'):
        """Generate report for all periods"""
        if not st.session_state.data_loaded:
            return {}
            
        report_periods = self.get_report_periods(period_type)
        all_period_data = {}
        
        if report_periods:
            for i, (period_name, start_date, end_date, period_type) in enumerate(report_periods):
                period_data = {
                    'period_name': period_name,
                    'start_date': start_date,
                    'end_date': end_date,
                    'period_type': period_type
                }
                
                # Calculate previous period for comparison
                prev_start = None
                prev_end = None
                if i > 0:
                    prev_start = report_periods[i-1][1]
                    prev_end = report_periods[i-1][2]
                
                # Calculate all metrics
                period_data['executive_snapshot'] = self.calculate_executive_snapshot(
                    start_date, end_date, period_type, prev_start, prev_end
                )
                
                period_data['customer_acquisition'] = self.calculate_customer_acquisition(
                    start_date, end_date, prev_start, prev_end
                )
                
                period_data['product_usage'] = self.calculate_product_usage_performance(
                    start_date, end_date, period_type, prev_start, prev_end
                )
                
                period_data['product_penetration'] = self.calculate_product_penetration(
                    start_date, end_date, period_type
                )
                
                period_data['customer_activity'] = self.calculate_customer_activity_engagement(
                    start_date, end_date, period_type
                )
                
                all_period_data[period_name] = period_data
        
        return all_period_data
    
    def display_dashboard(self, all_period_data):
        """Display the main dashboard"""
        st.title("📊 Business Development Performance Dashboard")
        
        # Date range info
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Report Start", st.session_state.start_date.strftime('%Y-%m-%d'))
        with col2:
            st.metric("Report End", st.session_state.end_date.strftime('%Y-%m-%d'))
        with col3:
            st.metric("Total Periods", len(all_period_data))
        
        st.divider()
        
        if all_period_data:
            # Period selection
            period_names = list(all_period_data.keys())
            
            if period_names:
                selected_period = st.selectbox(
                    "Select Period to View Details",
                    period_names,
                    index=len(period_names)-1 if period_names else 0
                )
                
                if selected_period:
                    period_data = all_period_data[selected_period]
                    self.display_period_details(period_data, selected_period)
        else:
            st.warning("No data available for analysis")
    
    def display_period_details(self, period_data, period_name):
        """Display detailed metrics for a specific period"""
        st.header(f"📋 {period_name}")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📅 Period Information")
            st.write(f"**Start Date:** {period_data['start_date'].strftime('%Y-%m-%d')}")
            st.write(f"**End Date:** {period_data['end_date'].strftime('%Y-%m-%d')}")
            st.write(f"**Period Type:** {period_data['period_type'].capitalize()}")
        
        with col2:
            st.subheader("🎯 Key Metrics")
            exec_snapshot = period_data.get('executive_snapshot', {})
            
            metric_col1, metric_col2 = st.columns(2)
            with metric_col1:
                st.metric("New Customers", exec_snapshot.get('new_customers_total', 0))
                active_customers = exec_snapshot.get('active_customers_all', 0)
                st.metric("Active Customers", active_customers, 
                         help="Customers who did at least 1 transaction in the period")
            
            with metric_col2:
                top_product = exec_snapshot.get('top_product', 'N/A')
                top_count = exec_snapshot.get('top_product_count', 0)
                st.metric("Top Product", f"{top_product}", delta=f"{top_count} txn")
        
        # Detailed metrics in expanders
        with st.expander("📊 1. Executive Snapshot Details", expanded=True):
            self.display_executive_details(period_data)
        
        with st.expander("👥 2. Customer Acquisition Details", expanded=True):
            self.display_acquisition_details(period_data)
        
        with st.expander("📱 3. Product Usage Performance", expanded=True):
            self.display_product_usage_details(period_data)
        
        with st.expander("🎯 4. Product Penetration", expanded=True):
            self.display_product_penetration_details(period_data)
        
        with st.expander("⚡ 5. Customer Activity & Engagement", expanded=True):
            self.display_activity_details(period_data)
    
    def display_executive_details(self, period_data):
        """Display executive snapshot details"""
        exec_snapshot = period_data.get('executive_snapshot', {})
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.subheader("👥 New Customers (Segmented)")
            st.metric("Active", exec_snapshot.get('new_customers_active', 0))
            st.metric("Registered", exec_snapshot.get('new_customers_registered', 0))
            st.metric("Temporary", exec_snapshot.get('new_customers_temporary', 0))
            st.metric("Total", exec_snapshot.get('new_customers_total', 0))
        
        with col2:
            st.subheader("📈 Performance Metrics")
            st.metric("Active Customers (All)", exec_snapshot.get('active_customers_all', 0),
                     help="Weekly: ≥2 transactions, Monthly: ≥10 transactions")
            
            wau_total = exec_snapshot.get('wau_total', 0)
            st.metric("WAU New Customers", wau_total)
            
            net_growth = exec_snapshot.get('net_growth_pct', None)
            if net_growth is not None:
                st.metric("Net Growth %", f"{net_growth:.1f}%")
            else:
                st.metric("Net Growth %", "N/A")
        
        with col3:
            st.subheader("📊 Product Performance")
            
            top_product = exec_snapshot.get('top_product', 'N/A')
            top_count = exec_snapshot.get('top_product_count', 0)
            if top_product != 'N/A':
                st.metric("Top Product", f"{top_product}", delta=f"{top_count} txn")
            
            low_product = exec_snapshot.get('low_product', 'N/A')
            low_count = exec_snapshot.get('low_product_count', 0)
            if low_product != 'N/A':
                st.metric("Lowest Product", f"{low_product}", delta=f"{low_count} txn", delta_color="inverse")
            else:
                st.info("No product data available")
    
    def display_acquisition_details(self, period_data):
        """Display customer acquisition details"""
        acquisition = period_data.get('customer_acquisition', {})
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📝 Registrations")
            st.metric("New Registrations (Active)", acquisition.get('new_registrations_active', 0))
            st.metric("New Registrations (Registered)", acquisition.get('new_registrations_registered', 0))
            st.metric("New Registrations (Temporary)", acquisition.get('new_registrations_temporary', 0))
            st.metric("Total Registrations", acquisition.get('new_registrations_total', 0))
            st.metric("KYC Completed", acquisition.get('kyc_completed', 0))
            
            kyc_rate = acquisition.get('kyc_rate', 0)
            st.metric("KYC Rate", f"{kyc_rate:.1f}%")
        
        with col2:
            st.subheader("🚀 Activation")
            st.metric("First-Time Transactors", acquisition.get('ftt', 0))
            ftt_rate = acquisition.get('ftt_rate', 0)
            st.metric("FTT Rate", f"{ftt_rate:.1f}%")
            st.metric("Activation Rate", f"{acquisition.get('activation_rate', 0):.1f}%")
            st.metric("Reactivated Users", acquisition.get('reactivated_count', 0))
    
    def display_product_usage_details(self, period_data):
        """Display product usage details"""
        product_usage = period_data.get('product_usage', {})
        
        if not product_usage:
            st.info("No product usage data available for this period")
            return
        
        # Convert to DataFrame for display
        product_data = []
        for product, metrics in product_usage.items():
            product_data.append({
                'Product': product,
                'Category': metrics.get('category', ''),
                'Active Users': metrics.get('unique_users', 0),
                'Total Transactions': metrics.get('total_transactions', 0),
                'Transaction Value': f"${metrics.get('total_amount', 0):,.2f}",
                'Avg Value': f"${metrics.get('avg_amount', 0):,.2f}"
            })
        
        if product_data:
            df = pd.DataFrame(product_data)
            
            # Summary metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                total_transactions = df['Total Transactions'].sum()
                st.metric("Total Transactions", total_transactions)
            with col2:
                total_users = df['Active Users'].sum()
                st.metric("Total Active Users", total_users)
            with col3:
                total_value = sum([float(v.replace('$', '').replace(',', '')) for v in df['Transaction Value']])
                st.metric("Total Value", f"${total_value:,.2f}")
            
            st.subheader("📊 Product Performance")
            
            # Top products chart
            if len(df) > 0:
                top_products = df.nlargest(10, 'Total Transactions')
                fig = px.bar(
                    top_products,
                    x='Product',
                    y='Total Transactions',
                    color='Category',
                    title="Top 10 Products by Transactions"
                )
                fig.update_layout(xaxis_tickangle=-45, showlegend=True)
                st.plotly_chart(fig, use_container_width=True)
            
            # Detailed table
            st.subheader("📋 Detailed Product Metrics")
            st.dataframe(
                df.sort_values('Total Transactions', ascending=False),
                use_container_width=True,
                hide_index=True
            )
    
    def display_product_penetration_details(self, period_data):
        """Display product penetration details"""
        penetration_metrics, wau_all = period_data.get('product_penetration', ({}, 0))
        
        if not penetration_metrics:
            st.info("No product penetration data available")
            return
        
        st.metric("Weekly Active Users (Total)", wau_all)
        
        # Convert to DataFrame for display
        penetration_data = []
        for product, metrics in penetration_metrics.items():
            penetration_data.append({
                'Product': product,
                '% of WAU': f"{metrics.get('penetration_pct', 0):.1f}%",
                'Active Users': metrics.get('active_product_users', 0),
                'FTT Users': metrics.get('ftt_users_count', 0),
                'Repeat Users': metrics.get('repeat_users_count', 0),
                'Total Users': metrics.get('total_product_users', 0)
            })
        
        if penetration_data:
            df = pd.DataFrame(penetration_data)
            
            # Chart for top products by penetration
            if len(df) > 0:
                top_penetration = df.nlargest(10, 'Active Users')
                fig = px.bar(
                    top_penetration,
                    x='Product',
                    y='Active Users',
                    title="Top 10 Products by Active Users",
                    color='% of WAU',
                    color_continuous_scale='Viridis'
                )
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            
            # Detailed table
            st.subheader("📋 Product Penetration Details")
            st.dataframe(
                df.sort_values('Active Users', ascending=False),
                use_container_width=True,
                hide_index=True
            )
    
    def display_activity_details(self, period_data):
        """Display customer activity details"""
        activity = period_data.get('customer_activity', {})
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Weekly Active Users", activity.get('wau', 0))
        
        with col2:
            st.metric("Total Transactions", activity.get('total_transactions', 0))
        
        with col3:
            avg_trans = activity.get('avg_transactions_per_user', 0)
            st.metric("Avg Transactions/User", f"{avg_trans:.2f}")
        
        with col4:
            avg_products = activity.get('avg_products_per_user', 0)
            st.metric("Avg Products/User", f"{avg_products:.2f}")

# Main Streamlit app
def main():
    st.sidebar.title("🔧 Dashboard Configuration")
    
    # Initialize dashboard
    dashboard = PerformanceDashboard()
    
    # Date range selector
    st.sidebar.subheader("📅 Date Range Selection")
    
    # Use session state dates as defaults
    start_date = st.sidebar.date_input(
        "Start Date",
        value=st.session_state.start_date,
        min_value=datetime(2024, 1, 1),
        max_value=datetime.now()
    )
    
    end_date = st.sidebar.date_input(
        "End Date",
        value=st.session_state.end_date,
        min_value=datetime(2024, 1, 1),
        max_value=datetime.now()
    )
    
    # Period type selector
    period_type = st.sidebar.selectbox(
        "Period Type",
        ["monthly", "weekly", "rolling"],
        index=0
    )
    
    # Load data button
    if st.sidebar.button("📥 Load Data", type="primary"):
        if start_date > end_date:
            st.sidebar.error("❌ Error: Start date cannot be after end date!")
            st.sidebar.info("Please adjust the dates so that Start Date is before End Date")
        else:
            with st.spinner("Loading data from database..."):
                if dashboard.load_data_from_db(start_date, end_date):
                    st.session_state.period_data = dashboard.generate_period_report(period_type)
    
    st.sidebar.divider()
    
    # Data summary
    if st.session_state.data_loaded:
        st.sidebar.subheader("📊 Data Summary")
        if st.session_state.transactions is not None:
            st.sidebar.metric("Transactions", f"{len(st.session_state.transactions):,}")
        if st.session_state.onboarding is not None and not st.session_state.onboarding.empty:
            st.sidebar.metric("Customers", f"{len(st.session_state.onboarding):,}")
    
    st.sidebar.divider()
    st.sidebar.markdown("### 📖 Instructions")
    st.sidebar.markdown("""
    1. Select date range (Start Date must be before End Date)
    2. Choose period type
    3. Click 'Load Data'
    4. Select period to view details
    5. Click expanders for metrics
    """)
    
    # Display dashboard if data is loaded
    if st.session_state.data_loaded and st.session_state.period_data:
        dashboard.display_dashboard(st.session_state.period_data)
    else:
        st.title("📊 Business Development Performance Dashboard")
        st.info("📥 Please select date range and click 'Load Data' to begin")
        
        # Show default date range
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Default Start Date", st.session_state.start_date.strftime('%Y-%m-%d'))
        with col2:
            st.metric("Default End Date", st.session_state.end_date.strftime('%Y-%m-%d'))

if __name__ == "__main__":
    main()
