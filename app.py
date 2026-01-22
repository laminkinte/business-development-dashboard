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

# Initialize session state for data persistence
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
        except Exception as e:
            st.error(f"❌ Database connection error")
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
            st.error(f"❌ Query execution error")
            return pd.DataFrame()

class PerformanceDashboard:
    def __init__(self):
        self.db = DatabaseManager()
        self.today = datetime.now()
        
        # Define date ranges
        self.start_date_overall = datetime(2025, 10, 1)
        self.end_date_overall = min(datetime(2026, 1, 14), self.today)
        
        # Product categories
        self.product_categories = {
            'P2P (Internal Wallet Transfer)': ['Internal Wallet Transfer'],
            'Cash-In': ['Deposit'],
            'Cash-Out': ['Scan To Withdraw Agent', 'Scan To Withdraw Customer', 'OTP Withdrawal'],
            'Disbursement': ['Disbursement'],
            'Cash Power': ['Nawec Cashpower'],
            'E-Ticketing': ['Ticket'],
            'Bank Transfers': ['BANK_TO_WALLET_TRANSFER', 'WALLET_TO_BANK_TRANSFER']
        }
        
        self.services = ['Airtime Topup']
        self.all_products = []
        for category, products in self.product_categories.items():
            self.all_products.extend(products)
        self.all_products.append('Airtime Topup')
        
        self.product_performance_history = {}
    
    def load_data_from_db(self):
        """Load data from MySQL database"""
        if not self.db.connect():
            return False
        
        try:
            # Show loading indicator
            progress_text = "Loading data from database..."
            progress_bar = st.progress(0, text=progress_text)
            
            # Load transaction data
            transaction_query = """
                SELECT 
                    id,
                    user_identifier,
                    entity_name,
                    product_name,
                    transaction_type,
                    amount,
                    status,
                    service_name,
                    ucp_name,
                    created_at,
                    sub_transaction_id
                FROM Transaction 
                WHERE created_at IS NOT NULL 
                AND created_at >= %s 
                AND created_at <= %s
            """
            
            progress_bar.progress(30, text="Loading transaction data...")
            transactions = self.db.execute_query(
                transaction_query, 
                (self.start_date_overall, self.end_date_overall)
            )
            
            if transactions.empty:
                st.warning("⚠️ No transaction data found")
                return False
            
            progress_bar.progress(60, text="Processing transaction data...")
            
            # Parse dates in transactions
            if 'created_at' in transactions.columns:
                transactions['created_at'] = pd.to_datetime(
                    transactions['created_at'], errors='coerce'
                )
            
            # Clean numeric columns
            if 'amount' in transactions.columns:
                transactions['amount'] = pd.to_numeric(
                    transactions['amount'].astype(str)
                    .str.replace(',', '')
                    .str.replace(' ', '')
                    .str.replace('D', '')
                    .str.replace('GMD', '')
                    .str.replace('$', '')
                    .str.replace('€', ''),
                    errors='coerce'
                )
            
            # Standardize column names
            transactions = transactions.rename(columns={
                'user_identifier': 'User_Identifier',
                'entity_name': 'Entity_Name',
                'product_name': 'Product_Name',
                'transaction_type': 'Transaction_Type',
                'amount': 'Amount',
                'status': 'Status',
                'service_name': 'Service_Name',
                'ucp_name': 'UCP_Name',
                'created_at': 'Created_At'
            })
            
            # Load onboarding data
            progress_bar.progress(80, text="Loading onboarding data...")
            
            onboarding_query = """
                SELECT 
                    mobile,
                    entity,
                    kyc_status,
                    registration_date,
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
            
            progress_bar.progress(90, text="Processing onboarding data...")
            
            if not onboarding.empty:
                # Parse dates in onboarding
                if 'registration_date' in onboarding.columns:
                    onboarding['registration_date'] = pd.to_datetime(
                        onboarding['registration_date'], errors='coerce'
                    )
                
                # Standardize column names
                onboarding = onboarding.rename(columns={
                    'mobile': 'Mobile',
                    'entity': 'Entity',
                    'kyc_status': 'KYC_Status',
                    'status': 'Status',
                    'registration_date': 'Registration_Date'
                })
                
                # Create User Identifier
                onboarding['User_Identifier'] = onboarding['Mobile'].astype(str).str.strip()
            else:
                onboarding = pd.DataFrame()
            
            self.db.disconnect()
            progress_bar.progress(100, text="Data loaded successfully!")
            
            # Store in session state
            st.session_state.transactions = transactions
            st.session_state.onboarding = onboarding
            st.session_state.data_loaded = True
            
            # Small delay to show completion
            import time
            time.sleep(0.5)
            progress_bar.empty()
            
            return True
            
        except Exception as e:
            st.error(f"❌ Error loading data")
            if self.db.connection:
                self.db.disconnect()
            return False
    
    def get_7day_rolling_periods(self):
        """Generate 7-day rolling periods"""
        rolling_periods = []
        start_date = self.start_date_overall
        
        while start_date <= self.end_date_overall:
            period_end = min(start_date + timedelta(days=6), self.end_date_overall)
            if (period_end - start_date).days >= 6:  # Only include full weeks
                period_name = f"{start_date.strftime('%b %d')} - {period_end.strftime('%b %d')}"
                rolling_periods.append((period_name, start_date, period_end, 'rolling'))
            start_date = start_date + timedelta(days=7)
        
        return rolling_periods[-8:] if len(rolling_periods) > 8 else rolling_periods
    
    def get_weekly_monthly_periods(self):
        """Get standard weekly and monthly periods"""
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
        
        return periods
    
    def get_report_periods(self):
        """Get all report periods"""
        periods = self.get_weekly_monthly_periods()
        rolling_periods = self.get_7day_rolling_periods()
        periods.extend(rolling_periods)
        return periods
    
    def filter_by_date_range(self, df, date_col, start_date, end_date):
        """Filter dataframe by date range"""
        if date_col not in df.columns or df.empty:
            return pd.DataFrame()
        
        mask = (df[date_col] >= start_date) & (df[date_col] <= end_date)
        return df[mask].copy()
    
    def calculate_executive_snapshot(self, start_date, end_date, period_type):
        """Calculate Executive Snapshot metrics"""
        metrics = {}
        
        # Filter onboarding for period
        period_onboarding = self.filter_by_date_range(
            st.session_state.onboarding, 'Registration_Date', start_date, end_date
        )
        
        # Get customer counts
        if not period_onboarding.empty and 'Entity' in period_onboarding.columns:
            customer_counts = {}
            for status in ['Active', 'Registered', 'TemporaryRegister']:
                status_customers = period_onboarding[
                    (period_onboarding['Entity'] == 'Customer') & 
                    (period_onboarding['Status'] == status)
                ]
                customer_counts[status] = status_customers['User_Identifier'].nunique() if 'User_Identifier' in status_customers.columns else 0
            
            metrics['new_customers_active'] = customer_counts.get('Active', 0)
            metrics['new_customers_registered'] = customer_counts.get('Registered', 0)
            metrics['new_customers_temporary'] = customer_counts.get('TemporaryRegister', 0)
            metrics['new_customers_total'] = sum(customer_counts.values())
        else:
            metrics['new_customers_active'] = 0
            metrics['new_customers_registered'] = 0
            metrics['new_customers_temporary'] = 0
            metrics['new_customers_total'] = 0
        
        # Get active customers
        period_transactions = self.filter_by_date_range(
            st.session_state.transactions, 'Created_At', start_date, end_date
        )
        
        if not period_transactions.empty:
            customer_transactions = period_transactions[
                (period_transactions['Entity_Name'] == 'Customer') &
                (period_transactions['Status'] == 'SUCCESS')
            ]
            
            if not customer_transactions.empty:
                user_transaction_counts = customer_transactions.groupby('User_Identifier').size()
                threshold = 2 if period_type in ['weekly', 'rolling'] else 10
                active_customers = (user_transaction_counts >= threshold).sum()
                metrics['active_customers_all'] = active_customers
            else:
                metrics['active_customers_all'] = 0
        else:
            metrics['active_customers_all'] = 0
        
        # Top performing products
        if not period_transactions.empty and 'Product_Name' in period_transactions.columns:
            customer_transactions = period_transactions[
                (period_transactions['Entity_Name'] == 'Customer') &
                (period_transactions['Status'] == 'SUCCESS')
            ]
            
            if not customer_transactions.empty:
                # Handle P2P transactions
                p2p_mask = (customer_transactions['Product_Name'] == 'Internal Wallet Transfer')
                if 'Transaction_Type' in customer_transactions.columns:
                    p2p_mask = p2p_mask & (customer_transactions['Transaction_Type'] == 'DR')
                if 'UCP_Name' in customer_transactions.columns:
                    p2p_mask = p2p_mask & (~customer_transactions['UCP_Name'].astype(str).str.contains('Fee', case=False, na=False))
                
                p2p_transactions = customer_transactions[p2p_mask]
                other_transactions = customer_transactions[
                    customer_transactions['Product_Name'] != 'Internal Wallet Transfer'
                ]
                
                # Combine transactions
                product_counts = pd.concat([
                    p2p_transactions['Product_Name'].value_counts(),
                    other_transactions['Product_Name'].value_counts()
                ]).groupby(level=0).sum()
                
                if not product_counts.empty:
                    product_counts = product_counts.sort_values(ascending=False)
                    metrics['top_product'] = product_counts.index[0]
                    metrics['top_product_count'] = int(product_counts.iloc[0])
                    
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
        
        return metrics
    
    def calculate_customer_acquisition(self, start_date, end_date):
        """Calculate Customer Acquisition metrics"""
        metrics = {}
        
        # Filter data for period
        period_onboarding = self.filter_by_date_range(
            st.session_state.onboarding, 'Registration_Date', start_date, end_date
        )
        
        period_transactions = self.filter_by_date_range(
            st.session_state.transactions, 'Created_At', start_date, end_date
        )
        
        # New registrations
        if not period_onboarding.empty:
            customer_onboarding = period_onboarding[period_onboarding['Entity'] == 'Customer']
            metrics['new_registrations_total'] = customer_onboarding['User_Identifier'].nunique() if 'User_Identifier' in customer_onboarding.columns else 0
            
            # KYC Completed
            if 'KYC_Status' in customer_onboarding.columns:
                kyc_completed = customer_onboarding[
                    customer_onboarding['KYC_Status'].str.upper() == 'VERIFIED'
                ]
                metrics['kyc_completed'] = kyc_completed['User_Identifier'].nunique() if 'User_Identifier' in kyc_completed.columns else 0
            else:
                metrics['kyc_completed'] = 0
        else:
            metrics['new_registrations_total'] = 0
            metrics['kyc_completed'] = 0
        
        # FTT calculation
        if not period_transactions.empty and 'new_registrations_total' in metrics and metrics['new_registrations_total'] > 0:
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
            customer_transactions = period_transactions[
                (period_transactions['Entity_Name'] == 'Customer') &
                (period_transactions['Status'] == 'SUCCESS')
            ]
            
            for category, products in self.product_categories.items():
                for product in products:
                    if product == 'Internal Wallet Transfer':
                        product_trans = customer_transactions[
                            (customer_transactions['Product_Name'] == product) &
                            (customer_transactions.get('Transaction_Type', '') == 'DR')
                        ]
                        if 'UCP_Name' in product_trans.columns:
                            product_trans = product_trans[
                                ~product_trans['UCP_Name'].astype(str).str.contains('Fee', case=False, na=False)
                            ]
                    else:
                        product_trans = customer_transactions[
                            customer_transactions['Product_Name'] == product
                        ]
                    
                    if not product_trans.empty:
                        user_counts = product_trans.groupby('User_Identifier').size()
                        threshold = 2 if period_type in ['weekly', 'rolling'] else 10
                        active_users = (user_counts >= threshold).sum()
                        
                        product_metrics[product] = {
                            'category': category,
                            'active_users': active_users,
                            'total_transactions': len(product_trans),
                            'total_amount': product_trans['Amount'].sum() if 'Amount' in product_trans.columns else 0,
                            'unique_users': product_trans['User_Identifier'].nunique()
                        }
                    else:
                        product_metrics[product] = {
                            'category': category,
                            'active_users': 0,
                            'total_transactions': 0,
                            'total_amount': 0,
                            'unique_users': 0
                        }
            
            # Airtime Topup
            for service in self.services:
                service_trans = customer_transactions[
                    (customer_transactions['Service_Name'] == service) &
                    (customer_transactions.get('Transaction_Type', '') == 'DR')
                ]
                
                if not service_trans.empty:
                    user_counts = service_trans.groupby('User_Identifier').size()
                    threshold = 2 if period_type in ['weekly', 'rolling'] else 10
                    active_users = (user_counts >= threshold).sum()
                    
                    product_metrics[service] = {
                        'category': 'Airtime Topup',
                        'active_users': active_users,
                        'total_transactions': len(service_trans),
                        'total_amount': service_trans['Amount'].sum() if 'Amount' in service_trans.columns else 0,
                        'unique_users': service_trans['User_Identifier'].nunique()
                    }
                else:
                    product_metrics[service] = {
                        'category': 'Airtime Topup',
                        'active_users': 0,
                        'total_transactions': 0,
                        'total_amount': 0,
                        'unique_users': 0
                    }
        
        return product_metrics
    
    def calculate_customer_activity(self, start_date, end_date, period_type):
        """Calculate Customer Activity metrics"""
        period_transactions = self.filter_by_date_range(
            st.session_state.transactions, 'Created_At', start_date, end_date
        )
        
        metrics = {}
        
        if not period_transactions.empty:
            customer_transactions = period_transactions[
                (period_transactions['Entity_Name'] == 'Customer') &
                (period_transactions['Status'] == 'SUCCESS')
            ]
            
            if not customer_transactions.empty:
                user_transaction_counts = customer_transactions.groupby('User_Identifier').size()
                threshold = 2 if period_type in ['weekly', 'rolling'] else 10
                
                # Active users
                active_users = user_transaction_counts[user_transaction_counts >= threshold]
                metrics['wau'] = len(active_users)
                metrics['total_transactions'] = len(customer_transactions)
                
                # Average transactions per active user
                if len(active_users) > 0:
                    metrics['avg_transactions_per_user'] = active_users.mean()
                else:
                    metrics['avg_transactions_per_user'] = 0
                
                # Average products per active user
                if len(active_users) > 0:
                    active_user_ids = active_users.index.tolist()
                    active_user_transactions = customer_transactions[
                        customer_transactions['User_Identifier'].isin(active_user_ids)
                    ]
                    products_per_user = active_user_transactions.groupby('User_Identifier')['Product_Name'].nunique()
                    metrics['avg_products_per_user'] = products_per_user.mean()
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
            progress_bar = st.progress(0, text="Generating reports...")
            total_periods = len(report_periods)
            
            for idx, (period_name, start_date, end_date, period_type) in enumerate(report_periods):
                progress_bar.progress((idx + 1) / total_periods, text=f"Processing {period_name}")
                
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
            
            progress_bar.empty()
        
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
            # Period selection in sidebar
            period_names = list(all_period_data.keys())
            
            # Add filters in sidebar
            st.sidebar.subheader("🔍 Filters")
            
            # Period type filter
            period_types = ['All', 'Monthly', 'Weekly', 'Rolling']
            selected_period_type = st.sidebar.selectbox(
                "Filter by Period Type",
                period_types,
                index=0
            )
            
            if selected_period_type != 'All':
                filtered_periods = {k: v for k, v in all_period_data.items() 
                                  if v['period_type'] == selected_period_type.lower()}
                if filtered_periods:
                    period_names = list(filtered_periods.keys())
                    all_period_data = filtered_periods
            
            selected_period = st.sidebar.selectbox(
                "Select Period",
                period_names,
                index=len(period_names)-1 if period_names else 0,
                key="period_selector"
            )
            
            if selected_period:
                period_data = all_period_data[selected_period]
                self.display_period_details(period_data, selected_period)
            
            # Overall summary
            st.divider()
            st.subheader("📈 Overall Trends & Analysis")
            
            # Create tabs for different views
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
                st.metric("Top Product", f"{top_product} ({top_count})")
        
        # Detailed metrics in expanders
        with st.expander("📊 Executive Snapshot Details", expanded=False):
            self.display_executive_details(period_data)
        
        with st.expander("👥 Customer Acquisition Details", expanded=False):
            self.display_acquisition_details(period_data)
        
        with st.expander("📱 Product Usage Details", expanded=False):
            self.display_product_details(period_data)
        
        with st.expander("⚡ Customer Activity Details", expanded=False):
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
            st.metric("Top Product", f"{top_product}", delta=f"{top_count} txn")
        
        with col3:
            st.subheader("📊 Product Ranking")
            
            if exec_snapshot.get('low_product', 'N/A') != 'N/A':
                low_product = exec_snapshot.get('low_product', 'N/A')
                low_count = exec_snapshot.get('low_product_count', 0)
                st.metric("Lowest Product", f"{low_product}", delta=f"{low_count} txn", delta_color="inverse")
    
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
            
            st.subheader("📊 Product Performance")
            
            tab1, tab2 = st.tabs(["📈 Transactions", "👥 Users"])
            
            with tab1:
                # Sort by transactions
                df_sorted = df.sort_values('Total Transactions', ascending=False).head(10)
                fig = px.bar(
                    df_sorted,
                    x='Product',
                    y='Total Transactions',
                    color='Category',
                    title="Top 10 Products by Transactions"
                )
                fig.update_layout(xaxis_tickangle=-45, showlegend=True)
                st.plotly_chart(fig, use_container_width=True)
            
            with tab2:
                # Sort by active users
                df_sorted = df.sort_values('Active Users', ascending=False).head(10)
                fig = px.bar(
                    df_sorted,
                    x='Product',
                    y='Active Users',
                    color='Category',
                    title="Top 10 Products by Active Users"
                )
                fig.update_layout(xaxis_tickangle=-45, showlegend=True)
                st.plotly_chart(fig, use_container_width=True)
    
    def display_activity_details(self, period_data):
        """Display customer activity details"""
        activity = period_data.get('customer_activity', {})
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Active Users (WAU)", activity.get('wau', 0))
        
        with col2:
            st.metric("Total Transactions", activity.get('total_transactions', 0))
        
        with col3:
            avg_trans = activity.get('avg_transactions_per_user', 0)
            st.metric("Avg Transactions/User", f"{avg_trans:.1f}")
    
    def display_executive_summary(self, all_period_data):
        """Display executive summary across all periods"""
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
        # Prepare data
        periods = []
        registrations = []
        ftt_counts = []
        
        for period_name, data in all_period_data.items():
            periods.append(period_name)
            acquisition = data.get('customer_acquisition', {})
            registrations.append(acquisition.get('new_registrations_total', 0))
            ftt_counts.append(acquisition.get('ftt', 0))
        
        analysis_df = pd.DataFrame({
            'Period': periods,
            'Registrations': registrations,
            'FTT Count': ftt_counts
        })
        
        # Chart
        fig = px.bar(
            analysis_df,
            x='Period',
            y=['Registrations', 'FTT Count'],
            title="Registrations vs FTT",
            barmode='group'
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    def display_product_analysis(self, all_period_data):
        """Display product analysis across all periods"""
        # Aggregate product performance across all periods
        product_performance = {}
        
        for period_name, data in all_period_data.items():
            product_usage = data.get('product_usage', {})
            
            for product, metrics in product_usage.items():
                if product not in product_performance:
                    product_performance[product] = {
                        'total_transactions': 0,
                        'total_users': 0,
                        'periods_active': 0,
                        'category': metrics.get('category', '')
                    }
                
                product_performance[product]['total_transactions'] += metrics.get('total_transactions', 0)
                product_performance[product]['total_users'] += metrics.get('unique_users', 0)
                product_performance[product]['periods_active'] += 1
        
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
            top_by_transactions = product_df.nlargest(10, 'Total Transactions')
            fig = px.bar(
                top_by_transactions,
                x='Product',
                y='Total Transactions',
                color='Category',
                title="Top 10 Products by Transactions"
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
                # Generate reports
                st.session_state.period_data = dashboard.generate_period_report()
            else:
                st.error("❌ Failed to load data from database")
    
    # Refresh button
    if st.sidebar.button("🔄 Refresh Data", type="secondary"):
        with st.spinner("Refreshing data..."):
            if dashboard.load_data_from_db():
                st.session_state.period_data = dashboard.generate_period_report()
                st.sidebar.success("✅ Data refreshed!")
            else:
                st.sidebar.error("❌ Failed to refresh data")
    
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
    1. Data loads automatically on startup
    2. Use filters in sidebar to refine view
    3. Select period to view details
    4. Click expanders for detailed metrics
    """)
    
    st.sidebar.divider()
    st.sidebar.markdown("### ℹ️ About")
    st.sidebar.markdown("""
    **Business Development Performance Dashboard**
    
    **Period:** Oct 2025 - Jan 2026
    **Database:** MySQL
    **Tables:** Transaction, Onboarding
    """)
    
    # Display dashboard if data is loaded
    if st.session_state.data_loaded and st.session_state.period_data:
        dashboard.display_dashboard(st.session_state.period_data)
    else:
        # Show initial message
        st.title("📊 Business Development Performance Dashboard")
        st.info("📥 Loading data... Please wait")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Database", "MySQL", "Connecting...")
        with col2:
            st.metric("Analysis Period", "Oct 2025 - Jan 2026")
        with col3:
            st.metric("Status", "Loading", "⏳")

if __name__ == "__main__":
    main()
