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
        except Exception:
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
        except Exception:
            return pd.DataFrame()

class PerformanceDashboard:
    def __init__(self):
        self.db = DatabaseManager()
        self.today = datetime.now()
        
        # Define date ranges
        self.start_date_overall = datetime(2025, 10, 1)
        self.end_date_overall = min(datetime(2026, 1, 14), self.today)
        
        # Product categories based on your database
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
    
    def load_data_from_db(self):
        """Load data from MySQL database"""
        if not self.db.connect():
            return False
        
        try:
            # Load transaction data with all columns
            transaction_query = """
                SELECT * FROM Transaction 
                WHERE created_at IS NOT NULL 
                AND created_at >= %s 
                AND created_at <= %s
                LIMIT 100000
            """
            
            transactions = self.db.execute_query(
                transaction_query, 
                (self.start_date_overall, self.end_date_overall)
            )
            
            if transactions.empty:
                return False
            
            # Standardize column names
            column_mapping = {}
            for col in transactions.columns:
                col_lower = col.lower().replace(' ', '_')
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
            required_columns = ['Created_At', 'Entity_Name', 'Status', 'User_Identifier']
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
                    col_lower = col.lower().replace(' ', '_')
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
            
            self.db.disconnect()
            
            # Store in session state
            st.session_state.transactions = transactions
            st.session_state.onboarding = onboarding if not onboarding.empty else pd.DataFrame()
            st.session_state.data_loaded = True
            
            return True
            
        except Exception:
            if self.db.connection:
                self.db.disconnect()
            return False
    
    def get_report_periods(self):
        """Get all report periods"""
        periods = []
        
        # Monthly periods
        monthly_dates = [
            ('October 2025', datetime(2025, 10, 1), datetime(2025, 10, 31)),
            ('November 2025', datetime(2025, 11, 1), datetime(2025, 11, 30)),
            ('December 2025', datetime(2025, 12, 1), datetime(2025, 12, 31))
        ]
        
        for name, start, end in monthly_dates:
            if start <= self.end_date_overall:
                periods.append((name, start, min(end, self.end_date_overall), 'monthly'))
        
        # Weekly periods for January 2026
        weekly_dates = [
            ('Jan Week 1', datetime(2026, 1, 1), datetime(2026, 1, 7)),
            ('Jan Week 2', datetime(2026, 1, 8), datetime(2026, 1, 14))
        ]
        
        for name, start, end in weekly_dates:
            if start <= self.end_date_overall:
                periods.append((name, start, min(end, self.end_date_overall), 'weekly'))
        
        # Add rolling periods
        rolling_periods = self.get_rolling_periods()
        periods.extend(rolling_periods)
        
        return periods
    
    def get_rolling_periods(self):
        """Generate rolling periods"""
        rolling_periods = []
        start_date = self.end_date_overall - timedelta(days=27)  # Last 28 days
        
        while start_date <= self.end_date_overall:
            period_end = min(start_date + timedelta(days=6), self.end_date_overall)
            if period_end > start_date:
                period_name = f"{start_date.strftime('%b %d')} - {period_end.strftime('%b %d')}"
                rolling_periods.append((period_name, start_date, period_end, 'rolling'))
            start_date = start_date + timedelta(days=7)
        
        return rolling_periods
    
    def filter_by_date_range(self, df, date_col, start_date, end_date):
        """Filter dataframe by date range"""
        if df is None or df.empty or date_col not in df.columns:
            return pd.DataFrame()
        
        mask = (df[date_col] >= start_date) & (df[date_col] <= end_date)
        return df[mask].copy()
    
    def calculate_executive_snapshot(self, start_date, end_date, period_type):
        """Calculate Executive Snapshot metrics"""
        metrics = {}
        
        # Get transactions for period
        period_transactions = self.filter_by_date_range(
            st.session_state.transactions, 'Created_At', start_date, end_date
        )
        
        # Get onboarding for period
        period_onboarding = self.filter_by_date_range(
            st.session_state.onboarding, 'Registration_Date', start_date, end_date
        )
        
        # New customers (from onboarding)
        if not period_onboarding.empty and 'Entity' in period_onboarding.columns:
            # Filter for customers only
            customer_onboarding = period_onboarding[period_onboarding['Entity'] == 'Customer']
            
            # Count by status
            if 'Status' in customer_onboarding.columns:
                active_count = len(customer_onboarding[customer_onboarding['Status'] == 'Active'])
                registered_count = len(customer_onboarding[customer_onboarding['Status'] == 'Registered'])
                temp_count = len(customer_onboarding[customer_onboarding['Status'] == 'TemporaryRegister'])
                
                metrics['new_customers_active'] = active_count
                metrics['new_customers_registered'] = registered_count
                metrics['new_customers_temporary'] = temp_count
                metrics['new_customers_total'] = len(customer_onboarding)
            else:
                metrics['new_customers_total'] = len(customer_onboarding)
                metrics['new_customers_active'] = 0
                metrics['new_customers_registered'] = 0
                metrics['new_customers_temporary'] = 0
        else:
            metrics['new_customers_total'] = 0
            metrics['new_customers_active'] = 0
            metrics['new_customers_registered'] = 0
            metrics['new_customers_temporary'] = 0
        
        # Active customers (from transactions)
        if not period_transactions.empty:
            # Filter successful customer transactions
            customer_transactions = period_transactions[
                (period_transactions['Entity_Name'] == 'Customer') &
                (period_transactions['Status'] == 'SUCCESS')
            ]
            
            if not customer_transactions.empty:
                # Count unique users with at least 1 transaction (more realistic)
                unique_customers = customer_transactions['User_Identifier'].nunique()
                metrics['active_customers_all'] = unique_customers
                
                # Top product
                if 'Product_Name' in customer_transactions.columns:
                    product_counts = customer_transactions['Product_Name'].value_counts()
                    if not product_counts.empty:
                        metrics['top_product'] = product_counts.index[0]
                        metrics['top_product_count'] = int(product_counts.iloc[0])
                        
                        # Find a product with transactions for low product
                        active_products = product_counts[product_counts > 0]
                        if not active_products.empty:
                            metrics['low_product'] = active_products.index[-1]
                            metrics['low_product_count'] = int(active_products.iloc[-1])
                        else:
                            metrics['low_product'] = 'N/A'
                            metrics['low_product_count'] = 0
                    else:
                        metrics['top_product'] = 'N/A'
                        metrics['low_product'] = 'N/A'
                else:
                    metrics['top_product'] = 'N/A'
                    metrics['low_product'] = 'N/A'
            else:
                metrics['active_customers_all'] = 0
                metrics['top_product'] = 'N/A'
                metrics['low_product'] = 'N/A'
        else:
            metrics['active_customers_all'] = 0
            metrics['top_product'] = 'N/A'
            metrics['low_product'] = 'N/A'
        
        return metrics
    
    def calculate_customer_acquisition(self, start_date, end_date):
        """Calculate Customer Acquisition metrics"""
        metrics = {}
        
        # Get onboarding for period
        period_onboarding = self.filter_by_date_range(
            st.session_state.onboarding, 'Registration_Date', start_date, end_date
        )
        
        # Get transactions for period
        period_transactions = self.filter_by_date_range(
            st.session_state.transactions, 'Created_At', start_date, end_date
        )
        
        # New registrations
        if not period_onboarding.empty:
            customer_onboarding = period_onboarding[period_onboarding['Entity'] == 'Customer']
            metrics['new_registrations_total'] = len(customer_onboarding)
            
            # KYC Completed
            if 'KYC_Status' in customer_onboarding.columns:
                kyc_completed = customer_onboarding[
                    customer_onboarding['KYC_Status'].str.upper() == 'VERIFIED'
                ]
                metrics['kyc_completed'] = len(kyc_completed)
            else:
                metrics['kyc_completed'] = 0
        else:
            metrics['new_registrations_total'] = 0
            metrics['kyc_completed'] = 0
        
        # FTT calculation
        if not period_transactions.empty and metrics['new_registrations_total'] > 0:
            # Get new customers who transacted
            new_customers = period_onboarding[
                (period_onboarding['Entity'] == 'Customer') & 
                period_onboarding['User_Identifier'].notna()
            ]['User_Identifier'].unique()
            
            customer_transactions = period_transactions[
                (period_transactions['Entity_Name'] == 'Customer') &
                (period_transactions['Status'] == 'SUCCESS')
            ]
            
            if not customer_transactions.empty and len(new_customers) > 0:
                ftt_customers = customer_transactions[
                    customer_transactions['User_Identifier'].isin(new_customers)
                ]['User_Identifier'].unique()
                metrics['ftt'] = len(ftt_customers)
            else:
                metrics['ftt'] = 0
        else:
            metrics['ftt'] = 0
        
        # FTT Rate
        if metrics['new_registrations_total'] > 0:
            metrics['ftt_rate'] = (metrics['ftt'] / metrics['new_registrations_total']) * 100
        else:
            metrics['ftt_rate'] = 0
        
        return metrics
    
    def calculate_product_usage(self, start_date, end_date, period_type):
        """Calculate Product Usage metrics"""
        period_transactions = self.filter_by_date_range(
            st.session_state.transactions, 'Created_At', start_date, end_date
        )
        
        product_metrics = {}
        
        if not period_transactions.empty:
            # Filter successful customer transactions
            customer_transactions = period_transactions[
                (period_transactions['Entity_Name'] == 'Customer') &
                (period_transactions['Status'] == 'SUCCESS')
            ]
            
            if not customer_transactions.empty:
                # Check what products we have
                if 'Product_Name' in customer_transactions.columns:
                    unique_products = customer_transactions['Product_Name'].dropna().unique()
                    
                    # Check each product in our categories
                    for category, products in self.product_categories.items():
                        for product in products:
                            if product in unique_products:
                                product_trans = customer_transactions[
                                    customer_transactions['Product_Name'] == product
                                ]
                                
                                # Special handling for P2P
                                if product == 'Internal Wallet Transfer' and 'Transaction_Type' in product_trans.columns:
                                    product_trans = product_trans[product_trans['Transaction_Type'] == 'DR']
                                    if 'UCP_Name' in product_trans.columns:
                                        product_trans = product_trans[
                                            ~product_trans['UCP_Name'].astype(str).str.contains('Fee', case=False, na=False)
                                        ]
                                
                                if not product_trans.empty:
                                    unique_users = product_trans['User_Identifier'].nunique()
                                    total_transactions = len(product_trans)
                                    total_amount = product_trans['Amount'].sum() if 'Amount' in product_trans.columns else 0
                                    
                                    product_metrics[product] = {
                                        'category': category,
                                        'active_users': unique_users,  # Using unique users as active users
                                        'total_transactions': total_transactions,
                                        'total_amount': total_amount,
                                        'unique_users': unique_users
                                    }
                
                # Check for Airtime Topup in Service_Name
                if 'Service_Name' in customer_transactions.columns:
                    service_trans = customer_transactions[
                        customer_transactions['Service_Name'] == 'Airtime Topup'
                    ]
                    
                    if not service_trans.empty:
                        unique_users = service_trans['User_Identifier'].nunique()
                        total_transactions = len(service_trans)
                        total_amount = service_trans['Amount'].sum() if 'Amount' in service_trans.columns else 0
                        
                        product_metrics['Airtime Topup'] = {
                            'category': 'Airtime',
                            'active_users': unique_users,
                            'total_transactions': total_transactions,
                            'total_amount': total_amount,
                            'unique_users': unique_users
                        }
        
        return product_metrics
    
    def calculate_customer_activity(self, start_date, end_date, period_type):
        """Calculate Customer Activity metrics"""
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
                # Active users (unique customers who transacted)
                unique_customers = customer_transactions['User_Identifier'].nunique()
                metrics['wau'] = unique_customers
                metrics['total_transactions'] = len(customer_transactions)
                
                # Average transactions per user
                if unique_customers > 0:
                    metrics['avg_transactions_per_user'] = len(customer_transactions) / unique_customers
                else:
                    metrics['avg_transactions_per_user'] = 0
                
                # Average products per user
                if unique_customers > 0 and 'Product_Name' in customer_transactions.columns:
                    products_per_user = customer_transactions.groupby('User_Identifier')['Product_Name'].nunique().mean()
                    metrics['avg_products_per_user'] = products_per_user
                else:
                    metrics['avg_products_per_user'] = 0
            else:
                metrics['wau'] = 0
                metrics['total_transactions'] = 0
                metrics['avg_transactions_per_user'] = 0
                metrics['avg_products_per_user'] = 0
        else:
            metrics['wau'] = 0
            metrics['total_transactions'] = 0
            metrics['avg_transactions_per_user'] = 0
            metrics['avg_products_per_user'] = 0
        
        return metrics
    
    def generate_period_report(self):
        """Generate report for all periods"""
        if not st.session_state.data_loaded:
            return {}
            
        report_periods = self.get_report_periods()
        all_period_data = {}
        
        if report_periods:
            for period_name, start_date, end_date, period_type in report_periods:
                period_data = {
                    'period_name': period_name,
                    'start_date': start_date,
                    'end_date': end_date,
                    'period_type': period_type
                }
                
                # Calculate all metrics
                period_data['executive_snapshot'] = self.calculate_executive_snapshot(
                    start_date, end_date, period_type
                )
                
                period_data['customer_acquisition'] = self.calculate_customer_acquisition(
                    start_date, end_date
                )
                
                period_data['product_usage'] = self.calculate_product_usage(
                    start_date, end_date, period_type
                )
                
                period_data['customer_activity'] = self.calculate_customer_activity(
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
            st.metric("Report Start", self.start_date_overall.strftime('%Y-%m-%d'))
        with col2:
            st.metric("Report End", self.end_date_overall.strftime('%Y-%m-%d'))
        with col3:
            st.metric("Total Periods", len(all_period_data))
        
        st.divider()
        
        if all_period_data:
            # Sidebar filters
            st.sidebar.subheader("🔍 Filters")
            
            # Period type filter
            period_types = ['All'] + list(set([data['period_type'].capitalize() for data in all_period_data.values()]))
            selected_period_type = st.sidebar.selectbox(
                "Period Type",
                period_types,
                index=0
            )
            
            # Filter periods
            if selected_period_type != 'All':
                filtered_data = {k: v for k, v in all_period_data.items() 
                               if v['period_type'] == selected_period_type.lower()}
            else:
                filtered_data = all_period_data
            
            period_names = list(filtered_data.keys())
            
            if period_names:
                selected_period = st.sidebar.selectbox(
                    "Select Period",
                    period_names,
                    index=len(period_names)-1
                )
                
                if selected_period:
                    period_data = filtered_data[selected_period]
                    self.display_period_details(period_data, selected_period)
            
            # Overall summary
            st.divider()
            st.subheader("📈 Overall Trends & Analysis")
            
            tab1, tab2, tab3 = st.tabs(["📊 Executive Summary", "👥 Customer Analysis", "📱 Product Analysis"])
            
            with tab1:
                self.display_executive_summary(all_period_data)
            
            with tab2:
                self.display_customer_analysis(all_period_data)
            
            with tab3:
                self.display_product_analysis(all_period_data)
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
        with st.expander("📊 Executive Snapshot Details", expanded=True):
            self.display_executive_details(period_data)
        
        with st.expander("👥 Customer Acquisition Details", expanded=True):
            self.display_acquisition_details(period_data)
        
        with st.expander("📱 Product Usage Details", expanded=True):
            self.display_product_details(period_data)
        
        with st.expander("⚡ Customer Activity Details", expanded=True):
            self.display_activity_details(period_data)
    
    def display_executive_details(self, period_data):
        """Display executive snapshot details"""
        exec_snapshot = period_data.get('executive_snapshot', {})
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.subheader("👥 New Customers")
            st.metric("Active", exec_snapshot.get('new_customers_active', 0))
            st.metric("Registered", exec_snapshot.get('new_customers_registered', 0))
            st.metric("Temporary", exec_snapshot.get('new_customers_temporary', 0))
        
        with col2:
            st.subheader("📈 Performance")
            st.metric("Total New Customers", exec_snapshot.get('new_customers_total', 0))
            st.metric("Active Customers", exec_snapshot.get('active_customers_all', 0))
            
            top_product = exec_snapshot.get('top_product', 'N/A')
            top_count = exec_snapshot.get('top_product_count', 0)
            if top_product != 'N/A':
                st.metric("Top Product", f"{top_product}", delta=f"{top_count} txn")
        
        with col3:
            st.subheader("📊 Product Ranking")
            
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
            st.metric("New Registrations", acquisition.get('new_registrations_total', 0))
            st.metric("KYC Completed", acquisition.get('kyc_completed', 0))
            
            if acquisition.get('new_registrations_total', 0) > 0:
                kyc_rate = (acquisition.get('kyc_completed', 0) / acquisition.get('new_registrations_total', 0)) * 100
                st.metric("KYC Rate", f"{kyc_rate:.1f}%")
        
        with col2:
            st.subheader("🚀 Activation")
            st.metric("First-Time Transactors", acquisition.get('ftt', 0))
            st.metric("FTT Rate", f"{acquisition.get('ftt_rate', 0):.1f}%")
    
    def display_product_details(self, period_data):
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
                'Active Users': metrics.get('active_users', 0),
                'Total Transactions': metrics.get('total_transactions', 0),
                'Transaction Value': f"${metrics.get('total_amount', 0):,.2f}",
                'Unique Users': metrics.get('unique_users', 0)
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
            
            tab1, tab2 = st.tabs(["📈 Transactions", "👥 Users"])
            
            with tab1:
                df_sorted = df.sort_values('Total Transactions', ascending=False).head(15)
                fig = px.bar(
                    df_sorted,
                    x='Product',
                    y='Total Transactions',
                    color='Category',
                    title="Top Products by Transactions"
                )
                fig.update_layout(xaxis_tickangle=-45, showlegend=True)
                st.plotly_chart(fig, use_container_width=True)
            
            with tab2:
                df_sorted = df.sort_values('Active Users', ascending=False).head(15)
                fig = px.bar(
                    df_sorted,
                    x='Product',
                    y='Active Users',
                    color='Category',
                    title="Top Products by Active Users"
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
    
    def display_activity_details(self, period_data):
        """Display customer activity details"""
        activity = period_data.get('customer_activity', {})
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Active Users (WAU)", activity.get('wau', 0))
        
        with col2:
            st.metric("Total Transactions", activity.get('total_transactions', 0))
        
        with col3:
            avg_trans = activity.get('avg_transactions_per_user', 0)
            st.metric("Avg Transactions/User", f"{avg_trans:.1f}")
        
        with col4:
            avg_products = activity.get('avg_products_per_user', 0)
            st.metric("Avg Products/User", f"{avg_products:.1f}")
    
    def display_executive_summary(self, all_period_data):
        """Display executive summary across all periods"""
        if not all_period_data:
            return
        
        # Prepare data for charts
        periods = []
        new_customers = []
        active_customers = []
        
        for period_name, data in all_period_data.items():
            periods.append(period_name)
            exec_snapshot = data.get('executive_snapshot', {})
            new_customers.append(exec_snapshot.get('new_customers_total', 0))
            active_customers.append(exec_snapshot.get('active_customers_all', 0))
        
        # Create summary DataFrame
        summary_df = pd.DataFrame({
            'Period': periods,
            'New Customers': new_customers,
            'Active Customers': active_customers
        })
        
        # Chart
        fig = px.line(
            summary_df,
            x='Period',
            y=['New Customers', 'Active Customers'],
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
        
        for period_name, data in all_period_data.items():
            periods.append(period_name)
            acquisition = data.get('customer_acquisition', {})
            registrations.append(acquisition.get('new_registrations_total', 0))
            ftt_counts.append(acquisition.get('ftt', 0))
            kyc_completed.append(acquisition.get('kyc_completed', 0))
        
        analysis_df = pd.DataFrame({
            'Period': periods,
            'Registrations': registrations,
            'FTT Count': ftt_counts,
            'KYC Completed': kyc_completed
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
                        'category': metrics.get('category', '')
                    }
                
                product_performance[product]['total_transactions'] += metrics.get('total_transactions', 0)
                product_performance[product]['total_users'] += metrics.get('unique_users', 0)
        
        if not product_performance:
            st.info("No product data available for analysis")
            return
        
        # Convert to DataFrame
        product_df = pd.DataFrame([
            {
                'Product': product,
                'Category': data['category'],
                'Total Transactions': data['total_transactions'],
                'Total Users': data['total_users']
            }
            for product, data in product_performance.items()
        ])
        
        # Top products
        st.subheader("🏆 Top Performing Products")
        
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

# Main Streamlit app
def main():
    st.sidebar.title("🔧 Dashboard Configuration")
    
    # Initialize dashboard
    dashboard = PerformanceDashboard()
    
    # Auto-load data on first run
    if not st.session_state.data_loaded:
        with st.spinner("📥 Loading data from database..."):
            if dashboard.load_data_from_db():
                st.session_state.period_data = dashboard.generate_period_report()
    
    # Refresh button
    if st.sidebar.button("🔄 Refresh Data", type="secondary"):
        with st.spinner("Refreshing data..."):
            if dashboard.load_data_from_db():
                st.session_state.period_data = dashboard.generate_period_report()
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
    1. Use filters in sidebar
    2. Select period to view details
    3. Click expanders for metrics
    4. Refresh for latest data
    """)
    
    # Display dashboard if data is loaded
    if st.session_state.data_loaded and st.session_state.period_data:
        dashboard.display_dashboard(st.session_state.period_data)
    else:
        st.title("📊 Business Development Performance Dashboard")
        st.info("📥 Loading data... Please wait")

if __name__ == "__main__":
    main()
