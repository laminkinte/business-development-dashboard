import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pymysql
import plotly.graph_objects as go
import plotly.express as px
import warnings
from io import BytesIO
import base64

warnings.filterwarnings('ignore')

# Set page configuration
st.set_page_config(
    page_title="Business Development Performance Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #1E3A8A;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #FFFFFF;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }
    .positive {
        color: #10B981;
        font-weight: bold;
    }
    .negative {
        color: #EF4444;
        font-weight: bold;
    }
    .neutral {
        color: #6B7280;
        font-weight: bold;
    }
    .period-selector {
        background-color: #F8FAFC;
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 1rem;
    }
    .stButton > button {
        width: 100%;
    }
</style>
""", unsafe_allow_html=True)

class PerformanceReportGenerator:
    def __init__(self):
        # Define dynamic date ranges
        self.today = datetime.now()
        
        # Base period: Oct 1, 2025 to Jan 14, 2026
        self.start_date_overall = datetime(2025, 10, 1)
        
        # Set end date as Jan 14, 2026 or today if earlier
        self.end_date_overall = min(datetime(2026, 1, 14), self.today)
        
        # Define all products and their categories
        self.product_categories = {
            'P2P (Internal Wallet Transfer)': ['Internal Wallet Transfer'],
            'Cash-In': ['Deposit'],
            'Cash-Out': ['Scan To Withdraw Agent', 'Scan To Withdraw Customer', 'OTP Withdrawal'],
            'Disbursement': ['Disbursement'],
            'Cash Power': ['Nawec Cashpower'],
            'E-Ticketing': ['Ticket'],
            'Bank Transfers': ['BANK_TO_WALLET_TRANSFER', 'WALLET_TO_BANK_TRANSFER']
        }
        
        # Airtime Topup is a Service, not Product
        self.services = ['Airtime Topup']
        
        # Flatten product list for filtering
        self.all_products = []
        for category, products in self.product_categories.items():
            self.all_products.extend(products)
        
        # Add service to all products list for reporting
        self.all_products.append('Airtime Topup')
        
        # Track product performance history for consistency analysis
        self.product_performance_history = {}
        
        # Database configuration (hardcoded in backend)
        self.db_config = {
            'host': 'db4free.net',
            'user': 'lamin_d_kinteh',
            'password': 'Lamin@123',
            'database': 'bdp_report',
            'port': 3306
        }
        
        self.transactions = pd.DataFrame()
        self.onboarding = pd.DataFrame()
        
        # Cache for performance
        self._cache = {}
    
    def connect_to_mysql(self):
        """Connect to MySQL database"""
        try:
            connection = pymysql.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                port=self.db_config.get('port', 3306),
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            return connection
        except Exception as e:
            st.error(f"Error connecting to MySQL: {e}")
            return None
    
    def load_data_from_mysql(self, start_date=None, end_date=None):
        """Load data from MySQL database with performance optimizations"""
        if start_date is None:
            start_date = self.start_date_overall
        if end_date is None:
            end_date = self.end_date_overall
        
        # Create cache key
        cache_key = f"data_{start_date}_{end_date}"
        
        # Check cache first
        if cache_key in self._cache:
            self.transactions, self.onboarding = self._cache[cache_key]
            st.success("✓ Loaded data from cache")
            self._display_data_quality()
            return True
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            connection = self.connect_to_mysql()
            if connection is None:
                return False
            
            # Load transaction data with optimized query
            status_text.text("Loading transaction data from MySQL...")
            progress_bar.progress(30)
            
            transaction_query = """
                SELECT 
                    user_identifier, entity_name, status, product_name,
                    service_name, transaction_type, amount, ucp_name,
                    created_at
                FROM Transaction
                WHERE created_at BETWEEN %s AND %s
                AND entity_name = 'Customer'
                AND status = 'SUCCESS'
            """
            
            with connection.cursor() as cursor:
                cursor.execute(transaction_query, (start_date, end_date))
                transaction_results = cursor.fetchall()
                self.transactions = pd.DataFrame(transaction_results)
            
            st.success(f"✓ Loaded {len(self.transactions)} transaction records")
            progress_bar.progress(60)
            
            # Load onboarding data with optimized query
            status_text.text("Loading onboarding data from MySQL...")
            
            onboarding_query = """
                SELECT 
                    mobile, entity, kyc_status, registration_date,
                    status
                FROM Onboarding
                WHERE registration_date BETWEEN %s AND %s
                AND entity = 'Customer'
            """
            
            with connection.cursor() as cursor:
                cursor.execute(onboarding_query, (start_date, end_date))
                onboarding_results = cursor.fetchall()
                self.onboarding = pd.DataFrame(onboarding_results)
            
            st.success(f"✓ Loaded {len(self.onboarding)} onboarding records")
            progress_bar.progress(90)
            
            # Clean and preprocess data
            status_text.text("Preprocessing data...")
            self._preprocess_data()
            
            progress_bar.progress(100)
            status_text.text("Data loading complete!")
            
            connection.close()
            
            # Cache the data
            self._cache[cache_key] = (self.transactions.copy(), self.onboarding.copy())
            
            # Display data quality metrics
            self._display_data_quality()
            
            return True
            
        except Exception as e:
            st.error(f"Error loading data from MySQL: {e}")
            return False
    
    def _preprocess_data(self):
        """Preprocess loaded data efficiently"""
        # Clean column names
        self.transactions.columns = self.transactions.columns.str.strip()
        self.onboarding.columns = self.onboarding.columns.str.strip()
        
        # Parse dates
        if 'created_at' in self.transactions.columns:
            self.transactions['created_at'] = pd.to_datetime(self.transactions['created_at'])
        
        if 'registration_date' in self.onboarding.columns:
            self.onboarding['registration_date'] = pd.to_datetime(self.onboarding['registration_date'])
        
        # Clean numeric columns
        if 'amount' in self.transactions.columns:
            self.transactions['amount'] = pd.to_numeric(self.transactions['amount'], errors='coerce')
        
        # Create User Identifier for merging
        if 'mobile' in self.onboarding.columns:
            self.onboarding['user_identifier'] = self.onboarding['mobile'].astype(str).str.strip()
    
    def _display_data_quality(self):
        """Display data quality metrics"""
        with st.expander("📊 Data Quality Summary", expanded=False):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Transactions", f"{len(self.transactions):,}")
                st.metric("Onboarding Records", f"{len(self.onboarding):,}")
            
            with col2:
                if 'status' in self.transactions.columns:
                    success_rate = (self.transactions['status'] == 'SUCCESS').mean() * 100
                    st.metric("Transaction Success Rate", f"{success_rate:.1f}%")
                
                if 'product_name' in self.transactions.columns:
                    unique_products = self.transactions['product_name'].nunique()
                    st.metric("Unique Products", unique_products)
            
            with col3:
                if 'status' in self.onboarding.columns:
                    active_users = (self.onboarding['status'] == 'Active').sum()
                    st.metric("Active Users", f"{active_users:,}")
                
                if 'kyc_status' in self.onboarding.columns:
                    verified_users = (self.onboarding['kyc_status'].str.upper() == 'VERIFIED').sum()
                    st.metric("KYC Verified", f"{verified_users:,}")
    
    def filter_by_date_range(self, df, date_col, start_date, end_date):
        """Filter dataframe by date range efficiently"""
        if date_col not in df.columns or df.empty:
            return pd.DataFrame()
        
        # Create cache key for filtered data
        cache_key = f"filtered_{date_col}_{start_date}_{end_date}_{len(df)}"
        if cache_key in self._cache:
            return self._cache[cache_key].copy()
        
        valid_dates = df[date_col].notna()
        mask = (df[date_col] >= start_date) & (df[date_col] <= end_date) & valid_dates
        filtered_df = df[mask].copy()
        
        # Cache the filtered result
        self._cache[cache_key] = filtered_df.copy()
        
        return filtered_df
    
    def get_new_registered_customers_segmented(self, start_date, end_date):
        """Get new registered customers segmented by Status"""
        cache_key = f"new_customers_{start_date}_{end_date}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        period_onboarding = self.filter_by_date_range(
            self.onboarding, 'registration_date', start_date, end_date
        )
        
        segmented_counts = {'Active': 0, 'Registered': 0, 'TemporaryRegister': 0, 'Total': 0}
        customer_lists = {'Active': [], 'Registered': [], 'TemporaryRegister': [], 'Total': []}
        
        if not period_onboarding.empty and 'entity' in period_onboarding.columns and 'status' in period_onboarding.columns:
            # Filter customers with status in ['Active', 'Registered', 'TemporaryRegister']
            valid_statuses = ['Active', 'Registered', 'TemporaryRegister']
            valid_customers = period_onboarding[
                (period_onboarding['entity'] == 'Customer') & 
                (period_onboarding['status'].isin(valid_statuses))
            ]
            
            # Segment by status
            for status in valid_statuses:
                status_customers = valid_customers[valid_customers['status'] == status]
                segmented_counts[status] = status_customers['user_identifier'].nunique()
                customer_lists[status] = status_customers['user_identifier'].unique().tolist()
            
            # Total
            segmented_counts['Total'] = valid_customers['user_identifier'].nunique()
            customer_lists['Total'] = valid_customers['user_identifier'].unique().tolist()
        
        result = (segmented_counts, customer_lists)
        self._cache[cache_key] = result
        return result
    
    def get_active_customers_all(self, start_date, end_date, period_type):
        """Get ALL active customers (not just new ones) based on period type"""
        cache_key = f"active_customers_{start_date}_{end_date}_{period_type}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        
        if not period_transactions.empty:
            # Different thresholds for different period types
            if period_type == 'weekly' or period_type == 'rolling':
                threshold = 2
            else:  # monthly
                threshold = 10
            
            # Count transactions per user efficiently
            user_transaction_counts = period_transactions['user_identifier'].value_counts()
            active_users = user_transaction_counts[user_transaction_counts >= threshold].index.tolist()
            
            result = (active_users, len(active_users))
        else:
            result = ([], 0)
        
        self._cache[cache_key] = result
        return result
    
    def calculate_executive_snapshot(self, start_date, end_date, period_type):
        """Calculate Executive Snapshot metrics WITH SEGMENTED CUSTOMERS"""
        cache_key = f"exec_snapshot_{start_date}_{end_date}_{period_type}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        metrics = {}
        
        # Get new registered customers SEGMENTED BY STATUS
        segmented_counts, segmented_lists = self.get_new_registered_customers_segmented(start_date, end_date)
        
        metrics['new_customers_active'] = segmented_counts['Active']
        metrics['new_customers_registered'] = segmented_counts['Registered']
        metrics['new_customers_temporary'] = segmented_counts['TemporaryRegister']
        metrics['new_customers_total'] = segmented_counts['Total']
        
        # Get ALL active customers (all customers, not just new ones)
        active_customers_all, active_count_all = self.get_active_customers_all(start_date, end_date, period_type)
        metrics['active_customers_all'] = active_count_all
        
        # Weekly Active Users (WAU) from new registered customers - BY STATUS
        wau_by_status = {'Active': 0, 'Registered': 0, 'TemporaryRegister': 0, 'Total': 0}
        
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        
        for status in ['Active', 'Registered', 'TemporaryRegister']:
            status_customers = segmented_lists[status]
            if status_customers and not period_transactions.empty:
                # Filter to status customers' successful transactions
                status_customer_transactions = period_transactions[
                    period_transactions['user_identifier'].isin(status_customers)
                ]
                
                if not status_customer_transactions.empty:
                    # Count transactions per status customer
                    status_customer_counts = status_customer_transactions['user_identifier'].value_counts()
                    
                    # Different thresholds for different period types
                    if period_type in ['weekly', 'rolling']:
                        threshold = 2
                    else:  # monthly
                        threshold = 10
                    
                    wau_by_status[status] = (status_customer_counts >= threshold).sum()
        
        metrics['wau_active'] = wau_by_status['Active']
        metrics['wau_registered'] = wau_by_status['Registered']
        metrics['wau_temporary'] = wau_by_status['TemporaryRegister']
        metrics['wau_total'] = sum(wau_by_status.values())
        
        # Net Customer Growth
        if period_type == 'weekly' or period_type == 'rolling':
            # Get previous week
            days_diff = (end_date - start_date).days + 1
            prev_start = start_date - timedelta(days=days_diff)
            prev_end = start_date - timedelta(seconds=1)
        else:  # monthly
            # Get previous month
            prev_start = start_date - timedelta(days=30)
            prev_end = start_date - timedelta(seconds=1)
        
        prev_segmented_counts, _ = self.get_new_registered_customers_segmented(prev_start, prev_end)
        
        if prev_segmented_counts['Total'] > 0:
            net_growth = ((segmented_counts['Total'] - prev_segmented_counts['Total']) / prev_segmented_counts['Total']) * 100
        else:
            net_growth = 0 if segmented_counts['Total'] > 0 else None
        
        metrics['net_growth_pct'] = net_growth
        
        # Top and Lowest Performing Products (by usage count)
        if not period_transactions.empty and 'product_name' in period_transactions.columns:
            # For P2P (Internal Wallet Transfer), we need special handling
            product_counts_dict = {}
            
            # Get regular products
            for product in period_transactions['product_name'].unique():
                if product == 'Internal Wallet Transfer':
                    # CORRECTED P2P COUNTING
                    p2p_transactions = period_transactions[
                        (period_transactions['product_name'] == 'Internal Wallet Transfer') &
                        (period_transactions['transaction_type'] == 'DR')
                    ]
                    
                    # Exclude fee transactions
                    if 'ucp_name' in p2p_transactions.columns:
                        p2p_transactions = p2p_transactions[
                            ~p2p_transactions['ucp_name'].str.contains('Fee', case=False, na=False)
                        ]
                    
                    product_counts_dict[product] = len(p2p_transactions)
                else:
                    product_transactions = period_transactions[
                        period_transactions['product_name'] == product
                    ]
                    product_counts_dict[product] = len(product_transactions)
            
            # Include Airtime Topup as a service
            if 'Airtime Topup' in self.services and 'service_name' in period_transactions.columns:
                airtime_transactions = period_transactions[
                    (period_transactions['service_name'] == 'Airtime Topup') &
                    (period_transactions['transaction_type'] == 'DR')
                ]
                product_counts_dict['Airtime Topup'] = len(airtime_transactions)
            
            # Convert to Series for sorting
            product_counts = pd.Series(product_counts_dict)
            product_counts = product_counts.sort_values(ascending=False)
            
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
        
        self._cache[cache_key] = metrics
        return metrics
    
    def calculate_customer_acquisition(self, start_date, end_date, previous_start=None, previous_end=None):
        """Calculate Customer Acquisition metrics with comparison"""
        cache_key = f"cust_acq_{start_date}_{end_date}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        metrics = {}
        
        # Get segmented customer counts
        segmented_counts, segmented_lists = self.get_new_registered_customers_segmented(start_date, end_date)
        
        # New Registrations by Status
        metrics['new_registrations_active'] = segmented_counts['Active']
        metrics['new_registrations_registered'] = segmented_counts['Registered']
        metrics['new_registrations_temporary'] = segmented_counts['TemporaryRegister']
        metrics['new_registrations_total'] = segmented_counts['Total']
        
        # KYC Completed (Status = Active and KYC Status = Verified)
        period_onboarding = self.filter_by_date_range(
            self.onboarding, 'registration_date', start_date, end_date
        )
        
        if not period_onboarding.empty and 'kyc_status' in period_onboarding.columns and 'status' in period_onboarding.columns:
            kyc_completed = period_onboarding[
                (period_onboarding['entity'] == 'Customer') &
                (period_onboarding['kyc_status'].str.upper() == 'VERIFIED') &
                (period_onboarding['status'] == 'Active')
            ]['user_identifier'].nunique()
        else:
            kyc_completed = 0
        metrics['kyc_completed'] = kyc_completed
        
        # First-Time Transactors (FTT) - New registered customers who transacted
        new_customers_total = segmented_lists['Total']
        if new_customers_total:
            period_transactions = self.filter_by_date_range(
                self.transactions, 'created_at', start_date, end_date
            )
            
            if not period_transactions.empty:
                # Find new customers who transacted
                transacting_new_customers = period_transactions[
                    period_transactions['user_identifier'].isin(new_customers_total)
                ]['user_identifier'].unique()
                
                ftt_count = len(transacting_new_customers)
            else:
                ftt_count = 0
        else:
            ftt_count = 0
        metrics['ftt'] = ftt_count
        
        self._cache[cache_key] = metrics
        return metrics
    
    def calculate_product_usage_performance(self, start_date, end_date, period_type, previous_start=None, previous_end=None):
        """Calculate Product Usage Performance metrics"""
        cache_key = f"product_usage_{start_date}_{end_date}_{period_type}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        
        product_metrics = {}
        
        if not period_transactions.empty:
            # Process regular products
            for category, products in self.product_categories.items():
                for product in products:
                    if product == 'Internal Wallet Transfer':
                        # CORRECTED P2P COUNTING
                        product_trans = period_transactions[
                            (period_transactions['product_name'] == 'Internal Wallet Transfer') &
                            (period_transactions['transaction_type'] == 'DR')
                        ]
                        
                        # Exclude fee transactions
                        if 'ucp_name' in product_trans.columns:
                            product_trans = product_trans[
                                ~product_trans['ucp_name'].str.contains('Fee', case=False, na=False)
                            ]
                    
                    else:
                        # For other products
                        product_trans = period_transactions[
                            period_transactions['product_name'] == product
                        ]
                    
                    if not product_trans.empty:
                        # Active Users
                        user_product_counts = product_trans['user_identifier'].value_counts()
                        
                        # Different thresholds for different period types
                        if period_type in ['weekly', 'rolling']:
                            threshold = 2
                        else:  # monthly
                            threshold = 10
                        
                        active_users_all = (user_product_counts >= threshold).sum()
                        
                        # Total metrics
                        total_transactions = len(product_trans)
                        total_amount = product_trans['amount'].sum() if 'amount' in product_trans.columns else 0
                        total_users = product_trans['user_identifier'].nunique()
                        avg_amount = total_amount / total_transactions if total_transactions > 0 else 0
                        
                        product_metrics[product] = {
                            'category': category,
                            'active_users_all': active_users_all,
                            'total_transactions': total_transactions,
                            'total_amount': total_amount,
                            'avg_amount': avg_amount,
                            'total_users': total_users,
                            'period_type': period_type
                        }
                    else:
                        product_metrics[product] = {
                            'category': category,
                            'active_users_all': 0,
                            'total_transactions': 0,
                            'total_amount': 0,
                            'avg_amount': 0,
                            'total_users': 0,
                            'period_type': period_type
                        }
            
            # Process Airtime Topup
            for service in self.services:
                if 'service_name' in period_transactions.columns:
                    service_trans = period_transactions[
                        (period_transactions['service_name'] == service) &
                        (period_transactions['transaction_type'] == 'DR')
                    ]
                    
                    if not service_trans.empty:
                        user_service_counts = service_trans['user_identifier'].value_counts()
                        
                        if period_type in ['weekly', 'rolling']:
                            threshold = 2
                        else:  # monthly
                            threshold = 10
                            
                        active_users_all = (user_service_counts >= threshold).sum()
                        
                        total_transactions = len(service_trans)
                        total_amount = service_trans['amount'].sum() if 'amount' in service_trans.columns else 0
                        total_users = service_trans['user_identifier'].nunique()
                        avg_amount = total_amount / total_transactions if total_transactions > 0 else 0
                        
                        product_metrics[service] = {
                            'category': 'Airtime Topup',
                            'active_users_all': active_users_all,
                            'total_transactions': total_transactions,
                            'total_amount': total_amount,
                            'avg_amount': avg_amount,
                            'total_users': total_users,
                            'period_type': period_type,
                            'trend': '→'
                        }
                    else:
                        product_metrics[service] = {
                            'category': 'Airtime Topup',
                            'active_users_all': 0,
                            'total_transactions': 0,
                            'total_amount': 0,
                            'avg_amount': 0,
                            'total_users': 0,
                            'period_type': period_type,
                            'trend': '→'
                        }
        
        self._cache[cache_key] = product_metrics
        return product_metrics
    
    def calculate_customer_activity_engagement(self, start_date, end_date, period_type):
        """Calculate Customer Activity & Engagement metrics"""
        cache_key = f"customer_activity_{start_date}_{end_date}_{period_type}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        
        metrics = {}
        
        if not period_transactions.empty:
            # Get active customers
            wau_active, wau_count = self.get_active_customers_all(start_date, end_date, period_type)
            metrics['wau'] = wau_count
            
            if wau_active:
                active_user_transactions = period_transactions[
                    period_transactions['user_identifier'].isin(wau_active)
                ]
                
                if not active_user_transactions.empty:
                    trans_per_active_user = active_user_transactions['user_identifier'].value_counts()
                    avg_transactions_per_user = trans_per_active_user.mean()
                    
                    products_per_active_user = active_user_transactions.groupby('user_identifier')['product_name'].nunique()
                    avg_products_per_user = products_per_active_user.mean()
                else:
                    avg_transactions_per_user = 0
                    avg_products_per_user = 0
            else:
                avg_transactions_per_user = 0
                avg_products_per_user = 0
            
            metrics.update({
                'avg_transactions_per_user': avg_transactions_per_user,
                'avg_products_per_user': avg_products_per_user,
                'total_transactions': len(period_transactions),
                'period_type': period_type
            })
        else:
            metrics = {
                'wau': 0,
                'avg_transactions_per_user': 0,
                'avg_products_per_user': 0,
                'total_transactions': 0,
                'period_type': period_type
            }
        
        self._cache[cache_key] = metrics
        return metrics

def display_executive_snapshot(metrics, period_name):
    """Display Executive Snapshot metrics"""
    st.markdown(f"<h3 class='sub-header'>Executive Snapshot - {period_name}</h3>", unsafe_allow_html=True)
    
    # Row 1: Main Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("New Customers (Total)", metrics.get('new_customers_total', 0))
        with st.expander("Segmented View"):
            st.metric("Active Status", metrics.get('new_customers_active', 0))
            st.metric("Registered Status", metrics.get('new_customers_registered', 0))
            st.metric("Temporary Status", metrics.get('new_customers_temporary', 0))
    
    with col2:
        st.metric("Active Customers", metrics.get('active_customers_all', 0))
        growth = metrics.get('net_growth_pct', 0)
        growth_display = f"{growth:.1f}%" if growth is not None else "N/A"
        delta = f"{growth:+.1f}%" if growth is not None else None
        st.metric("Net Growth %", growth_display, delta=delta)
    
    with col3:
        st.metric("WAU (New Customers)", metrics.get('wau_total', 0))
        with st.expander("WAU by Status"):
            st.metric("Active WAU", metrics.get('wau_active', 0))
            st.metric("Registered WAU", metrics.get('wau_registered', 0))
            st.metric("Temporary WAU", metrics.get('wau_temporary', 0))
    
    with col4:
        if metrics.get('top_product') != 'N/A':
            st.metric("Top Product", metrics.get('top_product', 'N/A'))
            st.metric("Transactions", metrics.get('top_product_count', 0))
        
        if metrics.get('low_product') != 'N/A' and metrics.get('low_product_count', 0) > 0:
            with st.expander("Lowest Product"):
                st.metric("Product", metrics.get('low_product', 'N/A'))
                st.metric("Transactions", metrics.get('low_product_count', 0))

def display_customer_acquisition(metrics):
    """Display Customer Acquisition metrics"""
    st.markdown("<h3 class='sub-header'>Customer Acquisition</h3>", unsafe_allow_html=True)
    
    # Row 1: Main Acquisition Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_reg = metrics.get('new_registrations_total', 0)
        st.metric("New Registrations", total_reg)
        with st.expander("By Status"):
            st.metric("Active", metrics.get('new_registrations_active', 0))
            st.metric("Registered", metrics.get('new_registrations_registered', 0))
            st.metric("Temporary", metrics.get('new_registrations_temporary', 0))
    
    with col2:
        kyc_completed = metrics.get('kyc_completed', 0)
        st.metric("KYC Completed", kyc_completed)
        kyc_rate = (kyc_completed / total_reg * 100) if total_reg > 0 else 0
        st.caption(f"KYC Rate: {kyc_rate:.1f}%")
    
    with col3:
        ftt = metrics.get('ftt', 0)
        st.metric("First-Time Transactors", ftt)
        ftt_rate = (ftt / total_reg * 100) if total_reg > 0 else 0
        st.caption(f"FTT Rate: {ftt_rate:.1f}%")
    
    with col4:
        # Activation metrics placeholder
        st.metric("Customer Base Growth", f"{total_reg:,}")
        st.caption("New registrations added")

def display_product_usage(product_metrics):
    """Display Product Usage Performance"""
    st.markdown("<h3 class='sub-header'>Product Usage Performance</h3>", unsafe_allow_html=True)
    
    if not product_metrics:
        st.info("No product usage data available for this period.")
        return
    
    # Create dataframe for display
    product_data = []
    for product, metrics in product_metrics.items():
        product_data.append({
            'Product': product,
            'Category': metrics.get('category', ''),
            'Active Users': metrics.get('active_users_all', 0),
            'Transactions': metrics.get('total_transactions', 0),
            'Total Amount': metrics.get('total_amount', 0),
            'Avg Amount': metrics.get('avg_amount', 0),
            'Unique Users': metrics.get('total_users', 0)
        })
    
    df = pd.DataFrame(product_data)
    df = df.sort_values('Transactions', ascending=False)
    
    # Display products in a table
    st.dataframe(
        df,
        hide_index=True,
        width='stretch',
        column_config={
            "Product": st.column_config.TextColumn("Product", width="medium"),
            "Category": st.column_config.TextColumn("Category", width="small"),
            "Active Users": st.column_config.NumberColumn("Active Users", format="%d"),
            "Transactions": st.column_config.NumberColumn("Transactions", format="%d"),
            "Total Amount": st.column_config.NumberColumn("Total Amount", format="GMD %.2f"),
            "Avg Amount": st.column_config.NumberColumn("Avg Amount", format="GMD %.2f"),
            "Unique Users": st.column_config.NumberColumn("Unique Users", format="%d")
        }
    )
    
    # Visualizations
    if len(df) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            # Top products by transactions
            top_products = df.head(10).sort_values('Transactions')
            if len(top_products) > 0:
                fig = px.bar(
                    top_products,
                    y='Product',
                    x='Transactions',
                    title='Top Products by Transactions',
                    orientation='h',
                    color='Transactions',
                    color_continuous_scale='Blues'
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Product categories by active users
            category_data = df.groupby('Category')['Active Users'].sum().reset_index()
            if len(category_data) > 0:
                fig = px.pie(
                    category_data,
                    values='Active Users',
                    names='Category',
                    title='Active Users by Product Category',
                    hole=0.4
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

def display_customer_activity(metrics):
    """Display Customer Activity metrics"""
    st.markdown("<h3 class='sub-header'>Customer Activity & Engagement</h3>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Weekly Active Users", metrics.get('wau', 0))
    
    with col2:
        st.metric("Total Transactions", metrics.get('total_transactions', 0))
    
    with col3:
        st.metric("Avg Transactions/User", f"{metrics.get('avg_transactions_per_user', 0):.2f}")
    
    with col4:
        st.metric("Avg Products/User", f"{metrics.get('avg_products_per_user', 0):.2f}")

def main():
    """Main Streamlit application"""
    # Header
    st.markdown("<h1 class='main-header'>📊 Business Development Performance Dashboard</h1>", unsafe_allow_html=True)
    st.markdown("---")
    
    # Initialize session state for caching
    if 'generator' not in st.session_state:
        st.session_state.generator = PerformanceReportGenerator()
    
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    
    if 'current_period' not in st.session_state:
        st.session_state.current_period = None
    
    if 'metrics' not in st.session_state:
        st.session_state.metrics = {}
    
    # Sidebar for configuration
    with st.sidebar:
        st.markdown("### 📅 Date Range Selection")
        
        # Date range selection with better UX
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date",
                datetime(2025, 10, 1),
                min_value=datetime(2025, 1, 1),
                max_value=datetime.now()
            )
        with col2:
            end_date = st.date_input(
                "End Date",
                min(datetime(2026, 1, 14), datetime.now()),
                min_value=datetime(2025, 1, 1),
                max_value=datetime.now()
            )
        
        start_date = datetime.combine(start_date, datetime.min.time())
        end_date = datetime.combine(end_date, datetime.max.time())
        
        # Quick date presets
        st.markdown("#### 🚀 Quick Presets")
        preset_col1, preset_col2 = st.columns(2)
        
        with preset_col1:
            if st.button("Last 7 Days", width='stretch'):
                end_date = datetime.now()
                start_date = end_date - timedelta(days=7)
                st.rerun()
            
            if st.button("This Month", width='stretch'):
                end_date = datetime.now()
                start_date = datetime(end_date.year, end_date.month, 1)
                st.rerun()
        
        with preset_col2:
            if st.button("Last 30 Days", width='stretch'):
                end_date = datetime.now()
                start_date = end_date - timedelta(days=30)
                st.rerun()
            
            if st.button("Full Period", width='stretch'):
                start_date = datetime(2025, 10, 1)
                end_date = min(datetime(2026, 1, 14), datetime.now())
                st.rerun()
        
        # Period type selection
        period_type = st.selectbox(
            "Period Type",
            ['Monthly', 'Weekly', '7-Day Rolling'],
            index=0
        ).lower()
        
        # Load data button with better UX
        st.markdown("---")
        load_button = st.button(
            "🚀 Load Data & Generate Report",
            type="primary",
            width='stretch'
        )
        
        if load_button:
            st.session_state.data_loaded = False
            st.session_state.current_period = f"{start_date.date()} to {end_date.date()}"
            st.session_state.metrics = {}
            st.rerun()
        
        # Clear cache button
        if st.button("🔄 Clear Cache & Refresh", width='stretch'):
            st.session_state.generator._cache = {}
            st.session_state.data_loaded = False
            st.success("Cache cleared!")
            st.rerun()
        
        # Database connection info
        st.markdown("---")
        with st.expander("🔒 Database Connection Info", expanded=False):
            st.info("""
            **Connected to:** db4free.net  
            **Database:** bdp_report  
            **Tables:** Transaction, Onboarding  
            *Credentials are securely stored in the backend*
            """)
        
        # Info section
        st.markdown("---")
        with st.expander("ℹ️ About this Dashboard"):
            st.markdown("""
            **Dashboard Features:**
            - Executive performance snapshot
            - Customer acquisition analytics
            - Product usage performance
            - Customer activity metrics
            - Interactive visualizations
            
            **Active Customer Definitions:**
            - **Monthly**: ≥10 transactions in month
            - **Weekly/Rolling**: ≥2 transactions in period
            
            **Key Metrics:**
            - WAU: Weekly Active Users
            - FTT: First-Time Transactors
            - KYC Rate: Verified customers
            - Net Growth: Customer growth vs previous period
            """)
    
    # Main content area
    if st.session_state.data_loaded and st.session_state.metrics:
        # Display metrics from session state
        exec_metrics = st.session_state.metrics.get('exec_metrics')
        cust_acq_metrics = st.session_state.metrics.get('cust_acq_metrics')
        product_metrics = st.session_state.metrics.get('product_metrics')
        activity_metrics = st.session_state.metrics.get('activity_metrics')
        
        # Display metrics in tabs
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📈 Executive Snapshot", 
            "👥 Customer Acquisition", 
            "📊 Product Usage", 
            "📱 Customer Activity",
            "📥 Export Data"
        ])
        
        with tab1:
            display_executive_snapshot(exec_metrics, st.session_state.current_period)
        
        with tab2:
            display_customer_acquisition(cust_acq_metrics)
        
        with tab3:
            display_product_usage(product_metrics)
        
        with tab4:
            display_customer_activity(activity_metrics)
        
        with tab5:
            st.markdown("<h3 class='sub-header'>Export Data</h3>", unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Export transactions
                if not st.session_state.generator.transactions.empty:
                    csv = st.session_state.generator.transactions.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Transactions CSV",
                        data=csv,
                        file_name=f"transactions_{start_date.date()}_to_{end_date.date()}.csv",
                        mime="text/csv",
                        width='stretch'
                    )
            
            with col2:
                # Export onboarding
                if not st.session_state.generator.onboarding.empty:
                    csv = st.session_state.generator.onboarding.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Onboarding CSV",
                        data=csv,
                        file_name=f"onboarding_{start_date.date()}_to_{end_date.date()}.csv",
                        mime="text/csv",
                        width='stretch'
                    )
    
    elif load_button or (st.session_state.current_period and not st.session_state.data_loaded):
        # Load and calculate data
        with st.spinner(f"Loading data for {start_date.date()} to {end_date.date()}..."):
            success = st.session_state.generator.load_data_from_mysql(start_date, end_date)
        
        if success:
            with st.spinner("Calculating metrics..."):
                # Calculate all metrics
                exec_metrics = st.session_state.generator.calculate_executive_snapshot(start_date, end_date, period_type)
                
                # Calculate previous period for comparison
                days_diff = (end_date - start_date).days + 1
                prev_start = start_date - timedelta(days=days_diff)
                prev_end = start_date - timedelta(seconds=1)
                cust_acq_metrics = st.session_state.generator.calculate_customer_acquisition(start_date, end_date, prev_start, prev_end)
                
                product_metrics = st.session_state.generator.calculate_product_usage_performance(start_date, end_date, period_type, prev_start, prev_end)
                
                activity_metrics = st.session_state.generator.calculate_customer_activity_engagement(start_date, end_date, period_type)
                
                # Store in session state
                st.session_state.metrics = {
                    'exec_metrics': exec_metrics,
                    'cust_acq_metrics': cust_acq_metrics,
                    'product_metrics': product_metrics,
                    'activity_metrics': activity_metrics
                }
                st.session_state.data_loaded = True
            
            st.success("✅ Data loaded and metrics calculated successfully!")
            st.rerun()
        else:
            st.error("❌ Failed to load data. Please check your database connection.")
    
    else:
        # Show welcome message
        st.markdown("""
        ## Welcome to the Business Development Performance Dashboard!
        
        This dashboard provides comprehensive analytics for business development and marketing performance.
        
        ### 🚀 Getting Started
        
        1. **Select Date Range** in the sidebar
        2. **Choose Period Type** (Monthly/Weekly/Rolling)
        3. Click **"Load Data & Generate Report"** button
        
        ### 📊 Available Reports
        
        - **Executive Snapshot**: Key performance indicators
        - **Customer Acquisition**: Registration and activation metrics
        - **Product Usage**: Detailed product performance analysis
        - **Customer Activity**: User engagement metrics
        - **Data Export**: Download reports and raw data
        
        ### 🔒 Database Connection
        
        The dashboard is pre-configured to connect to:
        - **Host**: db4free.net
        - **Database**: bdp_report
        - **Tables**: Transaction, Onboarding
        
        *Credentials are securely stored in the backend*
        """)
        
        # Quick stats about the dashboard
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Report Period", "Oct 2025 - Jan 2026")
        
        with col2:
            st.metric("Data Sources", "2 Tables")
        
        with col3:
            st.metric("Period Types", "3 Types")

if __name__ == "__main__":
    main()
