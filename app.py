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
        
        # Define product categories
        self.product_categories = {
            'P2P (Internal Wallet Transfer)': ['Internal Wallet Transfer'],
            'Cash-In': ['Deposit'],
            'Cash-Out': ['Scan To Withdraw Agent', 'Scan To Withdraw Customer', 'OTP Withdrawal'],
            'Disbursement': ['Disbursement'],
            'Cash Power': ['Nawec Cashpower'],
            'E-Ticketing': ['Ticket'],
            'Bank Transfers': ['BANK_TO_WALLET_TRANSFER', 'WALLET_TO_BANK_TRANSFER'],
            'Airtime': ['Airtime Topup'],
            'International': ['International Remittance'],
            'Money Transfer': ['Scan to Send Money', 'Wallet to Non Wallet Transfer'],
            'Add Money': ['Add Money'],
            'APS PAY': ['APS PAY']
        }
        
        self.all_products = []
        for category, products in self.product_categories.items():
            self.all_products.extend(products)
    
    def set_date_range(self, start_date, end_date):
        """Set the date range for analysis"""
        self.start_date_overall = start_date
        self.end_date_overall = end_date
        st.session_state.start_date = start_date
        st.session_state.end_date = end_date
        st.session_state.date_range_set = True
    
    def load_data_from_db(self, start_date, end_date):
        """Load data from MySQL database for given date range"""
        self.set_date_range(start_date, end_date)
        
        if not self.db.connect():
            return False
        
        try:
            # Load transaction data with all columns
            transaction_query = """
                SELECT * FROM Transaction 
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
            
            # Standardize column names
            column_mapping = {}
            for col in transactions.columns:
                col_lower = str(col).lower().replace(' ', '_')
                if 'created' in col_lower:
                    column_mapping[col] = 'Created_At'
                elif 'product' in col_lower:
                    column_mapping[col] = 'Product_Name'
                elif 'service' in col_lower:
                    column_mapping[col] = 'Service_Name'
                elif 'entity' in col_lower and 'name' in col_lower:
                    column_mapping[col] = 'Entity_Name'
                elif 'user' in col_lower and 'identifier' in col_lower:
                    column_mapping[col] = 'User_Identifier'
                elif 'status' in col_lower:
                    column_mapping[col] = 'Status'
                elif 'amount' in col_lower:
                    column_mapping[col] = 'Amount'
                elif 'transaction' in col_lower and 'type' in col_lower:
                    column_mapping[col] = 'Transaction_Type'
                elif 'ucp' in col_lower:
                    column_mapping[col] = 'UCP_Name'
            
            transactions = transactions.rename(columns=column_mapping)
            
            # Ensure required columns exist
            required_columns = ['Created_At', 'Entity_Name', 'Status', 'User_Identifier', 'Product_Name']
            for col in required_columns:
                if col not in transactions.columns:
                    transactions[col] = None
            
            # Parse dates
            if 'Created_At' in transactions.columns:
                transactions['Created_At'] = pd.to_datetime(transactions['Created_At'], errors='coerce')
            
            # Clean amount
            if 'Amount' in transactions.columns:
                transactions['Amount'] = pd.to_numeric(
                    transactions['Amount'].astype(str).str.replace('[^0-9.-]', '', regex=True),
                    errors='coerce'
                )
            
            # Clean text columns
            text_columns = ['User_Identifier', 'Product_Name', 'Entity_Name', 'Transaction_Type', 
                          'UCP_Name', 'Service_Name', 'Status']
            for col in text_columns:
                if col in transactions.columns:
                    transactions[col] = transactions[col].astype(str).str.strip()
            
            # Load onboarding data
            onboarding_query = """
                SELECT * FROM Onboarding 
                WHERE registration_date IS NOT NULL 
                AND registration_date >= %s 
                AND registration_date <= %s
            """
            
            onboarding = self.db.execute_query(
                onboarding_query,
                (self.start_date_overall, self.end_date_overall)
            )
            
            if not onboarding.empty:
                # Standardize column names
                onboarding_columns = {}
                for col in onboarding.columns:
                    col_lower = str(col).lower().replace(' ', '_')
                    if 'registration' in col_lower:
                        onboarding_columns[col] = 'Registration_Date'
                    elif 'mobile' in col_lower:
                        onboarding_columns[col] = 'Mobile'
                    elif 'entity' in col_lower:
                        onboarding_columns[col] = 'Entity'
                    elif 'kyc' in col_lower:
                        onboarding_columns[col] = 'KYC_Status'
                    elif 'status' in col_lower:
                        onboarding_columns[col] = 'Status'
                
                onboarding = onboarding.rename(columns=onboarding_columns)
                
                # Parse dates
                if 'Registration_Date' in onboarding.columns:
                    onboarding['Registration_Date'] = pd.to_datetime(onboarding['Registration_Date'], errors='coerce')
                
                # Create User Identifier
                if 'Mobile' in onboarding.columns:
                    onboarding['User_Identifier'] = onboarding['Mobile'].astype(str).str.strip()
                
                # Clean text columns
                if 'Status' in onboarding.columns:
                    onboarding['Status'] = onboarding['Status'].astype(str).str.strip()
                if 'Entity' in onboarding.columns:
                    onboarding['Entity'] = onboarding['Entity'].astype(str).str.strip()
            
            self.db.disconnect()
            
            # Store in session state
            st.session_state.transactions = transactions
            st.session_state.onboarding = onboarding if not onboarding.empty else pd.DataFrame()
            st.session_state.data_loaded = True
            
            return True
            
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
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
                
                if current_date.month == week_end.month:
                    period_name = f"{current_date.strftime('%b')} Week {current_date.isocalendar()[1] - current_date.replace(day=1).isocalendar()[1] + 1}"
                else:
                    period_name = f"{current_date.strftime('%b %d')} - {week_end.strftime('%b %d')}"
                
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
        
        mask = (df[date_col] >= start_date) & (df[date_col] <= end_date)
        return df[mask].copy()
    
    def get_new_registered_customers_segmented(self, start_date, end_date):
        """Get new registered customers segmented by Status"""
        period_onboarding = self.filter_by_date_range(
            st.session_state.onboarding, 'Registration_Date', start_date, end_date
        )
        
        segmented_counts = {'Active': 0, 'Registered': 0, 'TemporaryRegister': 0, 'Total': 0}
        customer_lists = {'Active': [], 'Registered': [], 'TemporaryRegister': [], 'Total': []}
        
        if not period_onboarding.empty and 'Entity' in period_onboarding.columns and 'Status' in period_onboarding.columns:
            # Filter customers with status in ['Active', 'Registered', 'TemporaryRegister']
            valid_statuses = ['Active', 'Registered', 'TemporaryRegister']
            valid_customers = period_onboarding[
                (period_onboarding['Entity'] == 'Customer') & 
                (period_onboarding['Status'].isin(valid_statuses))
            ]
            
            # Segment by status
            for status in valid_statuses:
                status_customers = valid_customers[valid_customers['Status'] == status]
                segmented_counts[status] = status_customers['User_Identifier'].nunique()
                customer_lists[status] = status_customers['User_Identifier'].unique().tolist()
            
            # Total
            segmented_counts['Total'] = valid_customers['User_Identifier'].nunique()
            customer_lists['Total'] = valid_customers['User_Identifier'].unique().tolist()
        
        return segmented_counts, customer_lists
    
    def calculate_executive_snapshot(self, start_date, end_date, period_type, prev_start=None, prev_end=None):
        """Calculate Executive Snapshot metrics WITH SEGMENTED CUSTOMERS"""
        metrics = {}
        
        # Get new registered customers SEGMENTED BY STATUS
        segmented_counts, segmented_lists = self.get_new_registered_customers_segmented(start_date, end_date)
        
        metrics['new_customers_active'] = segmented_counts['Active']
        metrics['new_customers_registered'] = segmented_counts['Registered']
        metrics['new_customers_temporary'] = segmented_counts['TemporaryRegister']
        metrics['new_customers_total'] = segmented_counts['Total']
        
        # Get ALL active customers (all customers, not just new ones)
        period_transactions = self.filter_by_date_range(
            st.session_state.transactions, 'Created_At', start_date, end_date
        )
        
        active_customers_all = []
        if not period_transactions.empty:
            # Filter successful customer transactions
            customer_transactions = period_transactions[
                (period_transactions['Entity_Name'] == 'Customer') &
                (period_transactions['Status'] == 'SUCCESS')
            ]
            
            if not customer_transactions.empty:
                # Different thresholds for weekly vs monthly
                if period_type == 'weekly':
                    # Weekly: Customers with >=2 transactions
                    user_transaction_counts = customer_transactions.groupby('User_Identifier').size()
                    active_customers_all = user_transaction_counts[user_transaction_counts >= 2].index.tolist()
                else:  # monthly
                    # Monthly: Customers with >=10 transactions
                    user_transaction_counts = customer_transactions.groupby('User_Identifier').size()
                    active_customers_all = user_transaction_counts[user_transaction_counts >= 10].index.tolist()
        
        metrics['active_customers_all'] = len(active_customers_all)
        
        # Weekly Active Users (WAU) from new registered customers - BY STATUS
        wau_by_status = {'Active': 0, 'Registered': 0, 'TemporaryRegister': 0, 'Total': 0}
        
        for status in ['Active', 'Registered', 'TemporaryRegister']:
            status_customers = segmented_lists[status]
            if status_customers and not period_transactions.empty:
                # Filter to status customers' successful transactions
                status_customer_transactions = period_transactions[
                    (period_transactions['User_Identifier'].isin(status_customers)) &
                    (period_transactions['Entity_Name'] == 'Customer') &
                    (period_transactions['Status'] == 'SUCCESS')
                ]
                
                if not status_customer_transactions.empty:
                    # Count transactions per status customer
                    status_customer_counts = status_customer_transactions.groupby('User_Identifier').size()
                    
                    # Different thresholds for weekly vs monthly
                    if period_type == 'weekly':
                        # Weekly: Status customers with >=2 transactions
                        active_status_customers = status_customer_counts[status_customer_counts >= 2].index.tolist()
                    else:  # monthly
                        # Monthly: Status customers with >=10 transactions
                        active_status_customers = status_customer_counts[status_customer_counts >= 10].index.tolist()
                    
                    wau_by_status[status] = len(active_status_customers)
        
        metrics['wau_active'] = wau_by_status['Active']
        metrics['wau_registered'] = wau_by_status['Registered']
        metrics['wau_temporary'] = wau_by_status['TemporaryRegister']
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
        
        # Top and Lowest Performing Products (by usage count) - WITH CORRECTED P2P COUNTING
        if not period_transactions.empty and 'Product_Name' in period_transactions.columns:
            # Filter to customer transactions
            customer_transactions = period_transactions[
                (period_transactions['Entity_Name'] == 'Customer') &
                (period_transactions['Status'] == 'SUCCESS') &
                (period_transactions['Product_Name'].notna())
            ]
            
            if not customer_transactions.empty:
                # For P2P (Internal Wallet Transfer), we need special handling
                product_counts_dict = {}
                
                for product in customer_transactions['Product_Name'].unique():
                    if product == 'Internal Wallet Transfer':
                        # CORRECTED P2P COUNTING:
                        # 1. Only count DR ledger (customer debits)
                        # 2. Exclude fee transactions (UCP Name containing "Fee")
                        p2p_transactions = customer_transactions[
                            (customer_transactions['Product_Name'] == 'Internal Wallet Transfer') &
                            (customer_transactions['Transaction_Type'] == 'DR')
                        ]
                        
                        # Exclude fee transactions
                        if 'UCP_Name' in p2p_transactions.columns:
                            p2p_transactions = p2p_transactions[
                                ~p2p_transactions['UCP_Name'].astype(str).str.contains('Fee', case=False, na=False)
                            ]
                        
                        product_counts_dict[product] = len(p2p_transactions)
                    else:
                        # For other products, count all transactions
                        product_transactions = customer_transactions[
                            customer_transactions['Product_Name'] == product
                        ]
                        product_counts_dict[product] = len(product_transactions)
                
                # Also check for Airtime Topup in Service_Name
                if 'Service_Name' in customer_transactions.columns:
                    airtime_transactions = customer_transactions[
                        (customer_transactions['Service_Name'] == 'Airtime Topup') &
                        (customer_transactions['Transaction_Type'] == 'DR')
                    ]
                    if not airtime_transactions.empty:
                        product_counts_dict['Airtime Topup'] = len(airtime_transactions)
                
                # Convert to Series for sorting
                if product_counts_dict:
                    product_counts = pd.Series(product_counts_dict)
                    product_counts = product_counts.sort_values(ascending=False)
                    
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
        """Calculate Customer Acquisition metrics with WoW comparison"""
        metrics = {}
        
        # Filter onboarding for the period
        period_onboarding = self.filter_by_date_range(
            st.session_state.onboarding, 'Registration_Date', start_date, end_date
        )
        
        # Filter transactions for the period
        period_transactions = self.filter_by_date_range(
            st.session_state.transactions, 'Created_At', start_date, end_date
        )
        
        # Get segmented customer counts
        segmented_counts, segmented_lists = self.get_new_registered_customers_segmented(start_date, end_date)
        
        # New Registrations by Status
        metrics['new_registrations_active'] = segmented_counts['Active']
        metrics['new_registrations_registered'] = segmented_counts['Registered']
        metrics['new_registrations_temporary'] = segmented_counts['TemporaryRegister']
        metrics['new_registrations_total'] = segmented_counts['Total']
        
        # KYC Completed (Status = Active and KYC Status = Verified)
        if not period_onboarding.empty and 'KYC_Status' in period_onboarding.columns and 'Status' in period_onboarding.columns:
            kyc_completed = period_onboarding[
                (period_onboarding['Entity'] == 'Customer') &
                (period_onboarding['KYC_Status'].astype(str).str.upper() == 'VERIFIED') &
                (period_onboarding['Status'] == 'Active')
            ]['User_Identifier'].nunique()
        else:
            kyc_completed = 0
        metrics['kyc_completed'] = kyc_completed
        
        # First-Time Transactors (FTT) - New registered customers who transacted
        new_customers_total = segmented_lists['Total']
        if new_customers_total and not period_transactions.empty:
            # Get successful customer transactions
            customer_transactions = period_transactions[
                (period_transactions['Entity_Name'] == 'Customer') &
                (period_transactions['Status'] == 'SUCCESS')
            ]
            
            if not customer_transactions.empty:
                # Find new customers who transacted
                transacting_new_customers = customer_transactions[
                    customer_transactions['User_Identifier'].isin(new_customers_total)
                ]['User_Identifier'].unique()
                
                ftt_count = len(transacting_new_customers)
            else:
                ftt_count = 0
        else:
            ftt_count = 0
        metrics['ftt'] = ftt_count
        
        # FTT Rate
        if metrics['new_registrations_total'] > 0:
            metrics['ftt_rate'] = (metrics['ftt'] / metrics['new_registrations_total']) * 100
        else:
            metrics['ftt_rate'] = 0
        
        # Activation Rate (Customers who didn't transact in previous period but did in current period)
        if prev_start and prev_end:
            # Get previous period transactions
            prev_transactions = self.filter_by_date_range(
                st.session_state.transactions, 'Created_At', prev_start, prev_end
            )
            
            # Get current period transactions
            current_transactions = period_transactions
            
            if not prev_transactions.empty and not current_transactions.empty:
                # Get all customers who transacted in previous period
                prev_active_customers = prev_transactions[
                    (prev_transactions['Entity_Name'] == 'Customer') &
                    (prev_transactions['Status'] == 'SUCCESS')
                ]['User_Identifier'].unique()
                
                # Get all customers who transacted in current period
                current_active_customers = current_transactions[
                    (current_transactions['Entity_Name'] == 'Customer') &
                    (current_transactions['Status'] == 'SUCCESS')
                ]['User_Identifier'].unique()
                
                # Reactivated customers = active now but not in previous period
                reactivated_customers = np.setdiff1d(current_active_customers, prev_active_customers)
                
                # Total customer base (all registered customers before current period)
                if not st.session_state.onboarding.empty and 'Registration_Date' in st.session_state.onboarding.columns:
                    total_customers_before = st.session_state.onboarding[
                        (st.session_state.onboarding['Registration_Date'] < start_date) &
                        (st.session_state.onboarding['Entity'] == 'Customer')
                    ]['User_Identifier'].nunique()
                else:
                    total_customers_before = 0
                
                if total_customers_before > 0:
                    activation_rate = (len(reactivated_customers) / total_customers_before) * 100
                else:
                    activation_rate = 0
            else:
                activation_rate = 0
                reactivated_customers = []
        else:
            activation_rate = 0
            reactivated_customers = []
        
        metrics['activation_rate'] = activation_rate
        metrics['reactivated_count'] = len(reactivated_customers)
        
        # WoW comparison if previous period provided
        if prev_start and prev_end:
            wow_comparison = {}
            for metric in ['new_registrations_total', 'kyc_completed', 'ftt', 'activation_rate']:
                if metric in metrics:
                    current = metrics[metric] or 0
                    
                    # Calculate previous period value
                    prev_period_onboarding = self.filter_by_date_range(
                        st.session_state.onboarding, 'Registration_Date', prev_start, prev_end
                    )
                    
                    if metric == 'new_registrations_total':
                        prev_segmented_counts, _ = self.get_new_registered_customers_segmented(prev_start, prev_end)
                        previous = prev_segmented_counts['Total'] or 0
                    elif metric == 'kyc_completed':
                        if not prev_period_onboarding.empty and 'KYC_Status' in prev_period_onboarding.columns:
                            previous = prev_period_onboarding[
                                (prev_period_onboarding['Entity'] == 'Customer') &
                                (prev_period_onboarding['KYC_Status'].astype(str).str.upper() == 'VERIFIED') &
                                (prev_period_onboarding['Status'] == 'Active')
                            ]['User_Identifier'].nunique()
                        else:
                            previous = 0
                    elif metric == 'ftt':
                        prev_segmented_counts, prev_segmented_lists = self.get_new_registered_customers_segmented(prev_start, prev_end)
                        prev_new_customers = prev_segmented_lists['Total']
                        prev_transactions = self.filter_by_date_range(
                            st.session_state.transactions, 'Created_At', prev_start, prev_end
                        )
                        
                        if prev_new_customers and not prev_transactions.empty:
                            prev_ftt_customers = prev_transactions[
                                (prev_transactions['Entity_Name'] == 'Customer') &
                                (prev_transactions['Status'] == 'SUCCESS') &
                                (prev_transactions['User_Identifier'].isin(prev_new_customers))
                            ]['User_Identifier'].unique()
                            previous = len(prev_ftt_customers)
                        else:
                            previous = 0
                    elif metric == 'activation_rate':
                        previous = 0  # Simplified
                    
                    if previous > 0:
                        growth = ((current - previous) / previous) * 100
                    else:
                        growth = 0 if current > 0 else None
                    
                    wow_comparison[metric] = {
                        'current': current,
                        'previous': previous,
                        'growth': growth,
                        'trend': '↑' if growth and growth > 5 else '↓' if growth and growth < -5 else '→'
                    }
            
            metrics['wow_comparison'] = wow_comparison
        
        return metrics
    
    def calculate_product_usage_performance(self, start_date, end_date, period_type, prev_start=None, prev_end=None):
        """Calculate Product Usage Performance metrics - WITH CORRECTED P2P COUNTING"""
        period_transactions = self.filter_by_date_range(
            st.session_state.transactions, 'Created_At', start_date, end_date
        )
        
        product_metrics = {}
        
        if not period_transactions.empty:
            # Process regular products
            for category, products in self.product_categories.items():
                for product in products:
                    if product == 'Internal Wallet Transfer':
                        # CORRECTED P2P COUNTING:
                        # Product Name = Internal Wallet Transfer 
                        # Entity Name = Customer 
                        # Transaction Type = DR (ONLY DR, not CR)
                        # UCP Name excluding "Fee" (filter out fee transactions)
                        product_trans = period_transactions[
                            (period_transactions['Product_Name'] == 'Internal Wallet Transfer') &
                            (period_transactions['Entity_Name'] == 'Customer') &
                            (period_transactions['Status'] == 'SUCCESS') &
                            (period_transactions['Transaction_Type'] == 'DR')  # ONLY DR LEDGER
                        ]
                        
                        # Exclude fee transactions by filtering out UCP Name containing "Fee"
                        if 'UCP_Name' in product_trans.columns:
                            # Filter out rows where UCP Name contains "Fee" (case-insensitive)
                            product_trans = product_trans[
                                ~product_trans['UCP_Name'].astype(str).str.contains('Fee', case=False, na=False)
                            ]
                    
                    elif product == 'Airtime Topup':
                        # Airtime Topup: Service Name = Airtime Topup AND Transaction Type = DR for Customer
                        product_trans = period_transactions[
                            (period_transactions['Service_Name'] == 'Airtime Topup') &
                            (period_transactions['Entity_Name'] == 'Customer') &
                            (period_transactions['Status'] == 'SUCCESS') &
                            (period_transactions['Transaction_Type'] == 'DR')
                        ]
                    else:
                        # For other products
                        product_trans = period_transactions[
                            (period_transactions['Product_Name'] == product) &
                            (period_transactions['Entity_Name'] == 'Customer') &
                            (period_transactions['Status'] == 'SUCCESS')
                        ]
                    
                    if not product_trans.empty:
                        # Active Users (ALL customers with >=2 transactions for this product)
                        # Count transactions per user for this product
                        user_product_counts = product_trans.groupby('User_Identifier').size()
                        
                        # Users with >=2 transactions for this specific product (ALL customers)
                        active_users_all = (user_product_counts >= 2).sum()
                        
                        # Total metrics
                        total_transactions = len(product_trans)
                        total_amount = product_trans['Amount'].sum() if 'Amount' in product_trans.columns else 0
                        total_users = product_trans['User_Identifier'].nunique()
                        
                        product_metrics[product] = {
                            'category': category,
                            'active_users_all': int(active_users_all),
                            'total_transactions': int(total_transactions),
                            'total_amount': float(total_amount),
                            'total_users': int(total_users)
                        }
                    else:
                        product_metrics[product] = {
                            'category': category,
                            'active_users_all': 0,
                            'total_transactions': 0,
                            'total_amount': 0,
                            'total_users': 0
                        }
                    
                    # WoW trend if previous period provided
                    if prev_start and prev_end:
                        # Get previous period metrics
                        prev_period_transactions = self.filter_by_date_range(
                            st.session_state.transactions, 'Created_At', prev_start, prev_end
                        )
                        
                        if not prev_period_transactions.empty:
                            if product == 'Internal Wallet Transfer':
                                # Apply same P2P definition for previous period
                                prev_product_trans = prev_period_transactions[
                                    (prev_period_transactions['Product_Name'] == 'Internal Wallet Transfer') &
                                    (prev_period_transactions['Entity_Name'] == 'Customer') &
                                    (prev_period_transactions['Status'] == 'SUCCESS') &
                                    (prev_period_transactions['Transaction_Type'] == 'DR')  # ONLY DR
                                ]
                                
                                # Exclude fee transactions for previous period too
                                if 'UCP_Name' in prev_product_trans.columns:
                                    prev_product_trans = prev_product_trans[
                                        ~prev_product_trans['UCP_Name'].astype(str).str.contains('Fee', case=False, na=False)
                                    ]
                            elif product == 'Airtime Topup':
                                prev_product_trans = prev_period_transactions[
                                    (prev_period_transactions['Service_Name'] == 'Airtime Topup') &
                                    (prev_period_transactions['Entity_Name'] == 'Customer') &
                                    (prev_period_transactions['Status'] == 'SUCCESS') &
                                    (prev_period_transactions['Transaction_Type'] == 'DR')
                                ]
                            else:
                                prev_product_trans = prev_period_transactions[
                                    (prev_period_transactions['Product_Name'] == product) &
                                    (prev_period_transactions['Entity_Name'] == 'Customer') &
                                    (prev_period_transactions['Status'] == 'SUCCESS')
                                ]
                            
                            prev_transactions_count = len(prev_product_trans)
                        else:
                            prev_transactions_count = 0
                        
                        current_transactions = product_metrics[product]['total_transactions']
                        
                        if prev_transactions_count > 0:
                            if current_transactions > prev_transactions_count * 1.05:
                                trend = '↑'
                            elif current_transactions < prev_transactions_count * 0.95:
                                trend = '↓'
                            else:
                                trend = '→'
                        else:
                            trend = '↑' if current_transactions > 0 else '→'
                        
                        product_metrics[product]['wow_trend'] = trend
                    else:
                        product_metrics[product]['wow_trend'] = '→'
        
        return product_metrics
    
    def calculate_product_penetration(self, start_date, end_date, period_type):
        """Calculate Product Penetration metrics - WITH CORRECTED P2P COUNTING"""
        period_transactions = self.filter_by_date_range(
            st.session_state.transactions, 'Created_At', start_date, end_date
        )
        
        # Get new registered customers
        segmented_counts, segmented_lists = self.get_new_registered_customers_segmented(start_date, end_date)
        new_customers_total = segmented_lists['Total']
        
        # Get ALL active customers (all customers, not just new ones)
        active_customers_all = []
        wau_all = 0
        if not period_transactions.empty:
            # Filter successful customer transactions
            customer_transactions = period_transactions[
                (period_transactions['Entity_Name'] == 'Customer') &
                (period_transactions['Status'] == 'SUCCESS')
            ]
            
            if not customer_transactions.empty:
                # Different thresholds for weekly vs monthly
                if period_type == 'weekly':
                    # Weekly: Customers with >=2 transactions
                    user_transaction_counts = customer_transactions.groupby('User_Identifier').size()
                    active_customers_all = user_transaction_counts[user_transaction_counts >= 2].index.tolist()
                else:  # monthly
                    # Monthly: Customers with >=10 transactions
                    user_transaction_counts = customer_transactions.groupby('User_Identifier').size()
                    active_customers_all = user_transaction_counts[user_transaction_counts >= 10].index.tolist()
                
                wau_all = len(active_customers_all)
        
        penetration_metrics = {}
        
        # Key products for penetration analysis
        key_products = ['Internal Wallet Transfer', 'Deposit', 'Scan To Withdraw Agent', 'Scan To Withdraw Customer', 
                       'OTP Withdrawal', 'Nawec Cashpower', 'Ticket', 
                       'BANK_TO_WALLET_TRANSFER', 'WALLET_TO_BANK_TRANSFER', 'Airtime Topup']
        
        for product in key_products:
            if not period_transactions.empty:
                if product == 'Internal Wallet Transfer':
                    # CORRECTED P2P DEFINITION:
                    # Product Name = Internal Wallet Transfer 
                    # Entity Name = Customer 
                    # Transaction Type = DR (ONLY DR)
                    # UCP Name excluding "Fee"
                    product_trans = period_transactions[
                        (period_transactions['Product_Name'] == 'Internal Wallet Transfer') &
                        (period_transactions['Entity_Name'] == 'Customer') &
                        (period_transactions['Status'] == 'SUCCESS') &
                        (period_transactions['Transaction_Type'] == 'DR')  # ONLY DR
                    ]
                    
                    # Exclude fee transactions
                    if 'UCP_Name' in product_trans.columns:
                        product_trans = product_trans[
                            ~product_trans['UCP_Name'].astype(str).str.contains('Fee', case=False, na=False)
                        ]
                        
                elif product == 'Airtime Topup':
                    # Airtime Topup: Service Name = Airtime Topup AND Transaction Type = DR for Customer
                    product_trans = period_transactions[
                        (period_transactions['Service_Name'] == 'Airtime Topup') &
                        (period_transactions['Entity_Name'] == 'Customer') &
                        (period_transactions['Status'] == 'SUCCESS') &
                        (period_transactions['Transaction_Type'] == 'DR')
                    ]
                else:
                    # Other products
                    product_trans = period_transactions[
                        (period_transactions['Product_Name'] == product) &
                        (period_transactions['Entity_Name'] == 'Customer') &
                        (period_transactions['Status'] == 'SUCCESS')
                    ]
                
                if not product_trans.empty:
                    # Users who used this product
                    product_users = set(product_trans['User_Identifier'].unique())
                    
                    # Active Users of this product (those in WAU who used the product)
                    active_product_users = [user for user in product_users if user in active_customers_all]
                    
                    # % of Total Active Users (WAU) who used this product
                    penetration_pct = (len(active_product_users) / wau_all * 100) if wau_all > 0 else 0
                    
                    # FTT Users (First Time Users from new customers)
                    ftt_users = []
                    if new_customers_total:
                        ftt_users = [user for user in product_users if user in new_customers_total]
                    
                    # Repeat Users (used product more than once in period)
                    user_trans_counts = product_trans['User_Identifier'].value_counts()
                    repeat_users = list(user_trans_counts[user_trans_counts > 1].index)
                    
                    # Drop-off Rate (simplified - would need historical data)
                    drop_off_rate = 0
                    
                    penetration_metrics[product] = {
                        'penetration_pct': penetration_pct,
                        'ftt_users_count': len(ftt_users),
                        'repeat_users_count': len(repeat_users),
                        'drop_off_rate': drop_off_rate,
                        'total_product_users': len(product_users),
                        'active_product_users': len(active_product_users)
                    }
                else:
                    penetration_metrics[product] = {
                        'penetration_pct': 0,
                        'ftt_users_count': 0,
                        'repeat_users_count': 0,
                        'drop_off_rate': 0,
                        'total_product_users': 0,
                        'active_product_users': 0
                    }
            else:
                penetration_metrics[product] = {
                    'penetration_pct': 0,
                    'ftt_users_count': 0,
                    'repeat_users_count': 0,
                    'drop_off_rate': 0,
                    'total_product_users': 0,
                    'active_product_users': 0
                }
        
        return penetration_metrics, wau_all
    
    def calculate_customer_activity_engagement(self, start_date, end_date, period_type):
        """Calculate Customer Activity & Engagement metrics - OPTIMIZED"""
        period_transactions = self.filter_by_date_range(
            st.session_state.transactions, 'Created_At', start_date, end_date
        )
        
        metrics = {}
        
        if not period_transactions.empty:
            # Filter successful customer transactions
            customer_transactions = period_transactions[
                (period_transactions['Entity_Name'] == 'Customer') &
                (period_transactions['Status'] == 'SUCCESS')
            ]
            
            if not customer_transactions.empty:
                # Weekly Active Users (WAU) - based on period type
                active_customers_all = []
                if period_type == 'weekly':
                    # Weekly: Customers with >=2 transactions
                    user_transaction_counts = customer_transactions.groupby('User_Identifier').size()
                    active_customers_all = user_transaction_counts[user_transaction_counts >= 2].index.tolist()
                else:  # monthly
                    # Monthly: Customers with >=10 transactions
                    user_transaction_counts = customer_transactions.groupby('User_Identifier').size()
                    active_customers_all = user_transaction_counts[user_transaction_counts >= 10].index.tolist()
                
                metrics['wau'] = len(active_customers_all)
                
                # Average Transactions per Active User (for WAU users only)
                if active_customers_all:
                    active_user_transactions = customer_transactions[
                        customer_transactions['User_Identifier'].isin(active_customers_all)
                    ]
                    
                    if not active_user_transactions.empty:
                        trans_per_active_user = active_user_transactions.groupby('User_Identifier').size()
                        avg_transactions_per_user = trans_per_active_user.mean()
                    else:
                        avg_transactions_per_user = 0
                else:
                    avg_transactions_per_user = 0
                
                # Average Products per User (for WAU users only)
                if active_customers_all:
                    active_user_transactions = customer_transactions[
                        customer_transactions['User_Identifier'].isin(active_customers_all)
                    ]
                    
                    if not active_user_transactions.empty:
                        products_per_active_user = active_user_transactions.groupby('User_Identifier')['Product_Name'].nunique()
                        avg_products_per_user = products_per_active_user.mean()
                    else:
                        avg_products_per_user = 0
                else:
                    avg_products_per_user = 0
                
                # Dormant Users - SIMPLIFIED
                dormant_users = 0
                
                # Reactivated Users (simplified)
                reactivated_users = 0
                
                metrics.update({
                    'avg_transactions_per_user': avg_transactions_per_user,
                    'avg_products_per_user': avg_products_per_user,
                    'dormant_users': dormant_users,
                    'reactivated_users': reactivated_users,
                    'total_transactions': len(customer_transactions)
                })
            else:
                metrics = {
                    'wau': 0,
                    'avg_transactions_per_user': 0,
                    'avg_products_per_user': 0,
                    'dormant_users': 0,
                    'reactivated_users': 0,
                    'total_transactions': 0
                }
        else:
            metrics = {
                'wau': 0,
                'avg_transactions_per_user': 0,
                'avg_products_per_user': 0,
                'dormant_users': 0,
                'reactivated_users': 0,
                'total_transactions': 0
            }
        
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
                
                # Calculate previous period for WoW comparison
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
        
        # Date range info - use session state dates
        col1, col2, col3 = st.columns(3)
        with col1:
            if hasattr(self, 'start_date_overall') and self.start_date_overall is not None:
                st.metric("Report Start", self.start_date_overall.strftime('%Y-%m-%d'))
            else:
                st.metric("Report Start", st.session_state.start_date.strftime('%Y-%m-%d'))
        with col2:
            if hasattr(self, 'end_date_overall') and self.end_date_overall is not None:
                st.metric("Report End", self.end_date_overall.strftime('%Y-%m-%d'))
            else:
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
                    index=len(period_names)-1
                )
                
                if selected_period:
                    period_data = all_period_data[selected_period]
                    self.display_period_details(period_data, selected_period)
            
            # Overall summary
            st.divider()
            st.subheader("📈 Overall Trends & Analysis")
            
            tab1, tab2, tab3, tab4 = st.tabs(["📊 Executive Summary", "👥 Customer Analysis", "📱 Product Analysis", "📈 WoW Trends"])
            
            with tab1:
                self.display_executive_summary(all_period_data)
            
            with tab2:
                self.display_customer_analysis(all_period_data)
            
            with tab3:
                self.display_product_analysis(all_period_data)
            
            with tab4:
                self.display_wow_trends(all_period_data)
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
                st.metric("Active Customers", exec_snapshot.get('active_customers_all', 0))
            
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
            st.metric("Active Customers (All)", exec_snapshot.get('active_customers_all', 0))
            
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
            
            if acquisition.get('new_registrations_total', 0) > 0:
                kyc_rate = (acquisition.get('kyc_completed', 0) / acquisition.get('new_registrations_total', 0)) * 100
                st.metric("KYC Rate", f"{kyc_rate:.1f}%")
        
        with col2:
            st.subheader("🚀 Activation")
            st.metric("First-Time Transactors", acquisition.get('ftt', 0))
            ftt_rate = acquisition.get('ftt_rate', 0)
            st.metric("FTT Rate", f"{ftt_rate:.1f}%")
            st.metric("Activation Rate", f"{acquisition.get('activation_rate', 0):.1f}%")
            st.metric("Reactivated Users", acquisition.get('reactivated_count', 0))
            
            # WoW comparison
            wow = acquisition.get('wow_comparison', {})
            if wow:
                st.subheader("📊 WoW Comparison")
                for metric_key, metric_data in wow.items():
                    trend = metric_data.get('trend', '→')
                    growth = metric_data.get('growth', 0)
                    if growth is not None:
                        st.write(f"{metric_key.replace('_', ' ').title()}: {trend} ({growth:.1f}%)")
    
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
                'Active Users (≥2 txn)': metrics.get('active_users_all', 0),
                'Total Transactions': metrics.get('total_transactions', 0),
                'Transaction Value': f"${metrics.get('total_amount', 0):,.2f}",
                'Unique Users': metrics.get('total_users', 0),
                'WoW Trend': metrics.get('wow_trend', '→')
            })
        
        if product_data:
            df = pd.DataFrame(product_data)
            
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                total_transactions = df['Total Transactions'].sum()
                st.metric("Total Transactions", total_transactions)
            with col2:
                total_users = df['Active Users (≥2 txn)'].sum()
                st.metric("Total Active Users", total_users)
            with col3:
                total_value = sum([float(v.replace('$', '').replace(',', '')) for v in df['Transaction Value']])
                st.metric("Total Value", f"${total_value:,.2f}")
            with col4:
                upward_trends = len(df[df['WoW Trend'] == '↑'])
                st.metric("Products Growing", upward_trends)
            
            st.subheader("📊 Product Performance")
            
            tab1, tab2, tab3 = st.tabs(["📈 Transactions", "👥 Active Users", "💰 Transaction Value"])
            
            with tab1:
                df_sorted = df.sort_values('Total Transactions', ascending=False).head(15)
                fig = px.bar(
                    df_sorted,
                    x='Product',
                    y='Total Transactions',
                    color='Category',
                    title="Top Products by Transactions",
                    hover_data=['WoW Trend']
                )
                fig.update_layout(xaxis_tickangle=-45, showlegend=True)
                st.plotly_chart(fig, use_container_width=True)
            
            with tab2:
                df_sorted = df.sort_values('Active Users (≥2 txn)', ascending=False).head(15)
                fig = px.bar(
                    df_sorted,
                    x='Product',
                    y='Active Users (≥2 txn)',
                    color='Category',
                    title="Top Products by Active Users",
                    hover_data=['WoW Trend']
                )
                fig.update_layout(xaxis_tickangle=-45, showlegend=True)
                st.plotly_chart(fig, use_container_width=True)
            
            with tab3:
                df_sorted = df.copy()
                df_sorted['Transaction Value Num'] = df_sorted['Transaction Value'].apply(lambda x: float(x.replace('$', '').replace(',', '')))
                df_sorted = df_sorted.sort_values('Transaction Value Num', ascending=False).head(15)
                fig = px.bar(
                    df_sorted,
                    x='Product',
                    y='Transaction Value Num',
                    color='Category',
                    title="Top Products by Transaction Value",
                    hover_data=['WoW Trend']
                )
                fig.update_layout(xaxis_tickangle=-45, showlegend=True, yaxis_title="Transaction Value ($)")
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
            
            # Chart for penetration
            fig = px.bar(
                df.sort_values('Active Users', ascending=False).head(10),
                x='Product',
                y='Active Users',
                title="Product Penetration - Top 10 Products by Active Users",
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
        
        col5, col6 = st.columns(2)
        
        with col5:
            st.metric("Dormant Users (>90 days)", activity.get('dormant_users', 0))
        
        with col6:
            st.metric("Reactivated Users", activity.get('reactivated_users', 0))
    
    def display_executive_summary(self, all_period_data):
        """Display executive summary across all periods"""
        if not all_period_data:
            return
        
        # Prepare data for charts
        periods = []
        new_customers = []
        active_customers = []
        wau_customers = []
        
        for period_name, data in all_period_data.items():
            periods.append(period_name)
            exec_snapshot = data.get('executive_snapshot', {})
            new_customers.append(exec_snapshot.get('new_customers_total', 0))
            active_customers.append(exec_snapshot.get('active_customers_all', 0))
            wau_customers.append(exec_snapshot.get('wau_total', 0))
        
        # Create summary DataFrame
        summary_df = pd.DataFrame({
            'Period': periods,
            'New Customers': new_customers,
            'Active Customers': active_customers,
            'WAU New Customers': wau_customers
        })
        
        # Chart
        fig = px.line(
            summary_df,
            x='Period',
            y=['New Customers', 'Active Customers', 'WAU New Customers'],
            title="Customer Growth Trends",
            markers=True
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        
        # Period comparison table
        st.subheader("📋 Period Comparison")
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
    
    def display_customer_analysis(self, all_period_data):
        """Display customer analysis across all periods"""
        if not all_period_data:
            return
        
        # Prepare data
        periods = []
        registrations = []
        ftt_counts = []
        kyc_completed = []
        activation_rates = []
        
        for period_name, data in all_period_data.items():
            periods.append(period_name)
            acquisition = data.get('customer_acquisition', {})
            registrations.append(acquisition.get('new_registrations_total', 0))
            ftt_counts.append(acquisition.get('ftt', 0))
            kyc_completed.append(acquisition.get('kyc_completed', 0))
            activation_rates.append(acquisition.get('activation_rate', 0))
        
        analysis_df = pd.DataFrame({
            'Period': periods,
            'Registrations': registrations,
            'FTT Count': ftt_counts,
            'KYC Completed': kyc_completed,
            'Activation Rate %': activation_rates
        })
        
        # Chart
        fig = px.bar(
            analysis_df,
            x='Period',
            y=['Registrations', 'FTT Count', 'KYC Completed'],
            title="Customer Acquisition Metrics",
            barmode='group'
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        
        # Activation rate chart
        fig2 = px.line(
            analysis_df,
            x='Period',
            y='Activation Rate %',
            title="Activation Rate Trend",
            markers=True
        )
        fig2.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig2, use_container_width=True)
    
    def display_product_analysis(self, all_period_data):
        """Display product analysis across all periods"""
        # Aggregate product performance
        product_performance = {}
        
        for period_name, data in all_period_data.items():
            product_usage = data.get('product_usage', {})
            
            for product, metrics in product_usage.items():
                if product not in product_performance:
                    product_performance[product] = {
                        'total_transactions': 0,
                        'total_users': 0,
                        'total_amount': 0,
                        'category': metrics.get('category', '')
                    }
                
                product_performance[product]['total_transactions'] += metrics.get('total_transactions', 0)
                product_performance[product]['total_users'] += metrics.get('total_users', 0)
                product_performance[product]['total_amount'] += metrics.get('total_amount', 0)
        
        if not product_performance:
            st.info("No product data available for analysis")
            return
        
        # Convert to DataFrame
        product_df = pd.DataFrame([
            {
                'Product': product,
                'Category': data['category'],
                'Total Transactions': data['total_transactions'],
                'Total Active Users': data['total_users'],
                'Total Value': f"${data['total_amount']:,.2f}"
            }
            for product, data in product_performance.items()
        ])
        
        # Top products
        st.subheader("🏆 Top Performing Products (Overall)")
        
        col1, col2 = st.columns(2)
        
        with col1:
            top_by_transactions = product_df.nlargest(15, 'Total Transactions')
            fig = px.bar(
                top_by_transactions,
                x='Product',
                y='Total Transactions',
                color='Category',
                title="Top Products by Transactions (Overall)"
            )
            fig.update_layout(xaxis_tickangle=-45, showlegend=True)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            top_by_users = product_df.nlargest(15, 'Total Active Users')
            fig = px.bar(
                top_by_users,
                x='Product',
                y='Total Active Users',
                color='Category',
                title="Top Products by Active Users (Overall)"
            )
            fig.update_layout(xaxis_tickangle=-45, showlegend=True)
            st.plotly_chart(fig, use_container_width=True)
        
        # Detailed table
        st.subheader("📋 Overall Product Performance")
        st.dataframe(
            product_df.sort_values('Total Transactions', ascending=False),
            use_container_width=True,
            hide_index=True
        )
    
    def display_wow_trends(self, all_period_data):
        """Display WoW trends"""
        if len(all_period_data) < 2:
            st.info("Need at least 2 periods for WoW comparison")
            return
        
        # Prepare WoW comparison data
        wow_data = []
        periods = list(all_period_data.keys())
        
        for i in range(1, len(periods)):
            current_period = periods[i]
            previous_period = periods[i-1]
            
            current_data = all_period_data[current_period]
            previous_data = all_period_data[previous_period]
            
            # Compare key metrics
            current_exec = current_data.get('executive_snapshot', {})
            previous_exec = previous_data.get('executive_snapshot', {})
            
            wow_data.append({
                'Comparison': f'{current_period} vs {previous_period}',
                'New Customers': current_exec.get('new_customers_total', 0),
                'New Customers WoW': self._calculate_growth(
                    current_exec.get('new_customers_total', 0),
                    previous_exec.get('new_customers_total', 0)
                ),
                'Active Customers': current_exec.get('active_customers_all', 0),
                'Active Customers WoW': self._calculate_growth(
                    current_exec.get('active_customers_all', 0),
                    previous_exec.get('active_customers_all', 0)
                ),
                'WAU': current_exec.get('wau_total', 0),
                'WAU WoW': self._calculate_growth(
                    current_exec.get('wau_total', 0),
                    previous_exec.get('wau_total', 0)
                )
            })
        
        if wow_data:
            wow_df = pd.DataFrame(wow_data)
            st.dataframe(wow_df, use_container_width=True, hide_index=True)
            
            # Visualization
            fig = px.bar(
                wow_df,
                x='Comparison',
                y=['New Customers WoW', 'Active Customers WoW', 'WAU WoW'],
                title="WoW Growth Comparison (%)",
                barmode='group'
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
    
    def _calculate_growth(self, current, previous):
        """Calculate growth percentage"""
        if previous > 0:
            return ((current - previous) / previous) * 100
        return 0 if current > 0 else None

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
        with st.spinner("Loading data from database..."):
            if dashboard.load_data_from_db(start_date, end_date):
                st.session_state.period_data = dashboard.generate_period_report(period_type)
                st.sidebar.success("✅ Data loaded successfully!")
            else:
                st.sidebar.error("❌ Failed to load data")
    
    # Refresh button
    if st.sidebar.button("🔄 Refresh Data", type="secondary"):
        with st.spinner("Refreshing data..."):
            if dashboard.load_data_from_db(start_date, end_date):
                st.session_state.period_data = dashboard.generate_period_report(period_type)
                st.sidebar.success("✅ Data refreshed!")
    
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
    1. Select date range
    2. Choose period type
    3. Click 'Load Data'
    4. Select period to view details
    5. Click expanders for metrics
    6. Refresh for latest data
    """)
    
    # Display dashboard if data is loaded
    if st.session_state.data_loaded and st.session_state.period_data:
        dashboard.display_dashboard(st.session_state.period_data)
    else:
        st.title("📊 Business Development Performance Dashboard")
        st.info("📥 Please select date range and click 'Load Data' to begin")
        
        # Show default date range if no data loaded
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Default Start Date", st.session_state.start_date.strftime('%Y-%m-%d'))
        with col2:
            st.metric("Default End Date", st.session_state.end_date.strftime('%Y-%m-%d'))

if __name__ == "__main__":
    main()
