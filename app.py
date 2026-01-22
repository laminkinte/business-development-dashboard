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
    def __init__(self, db_config=None):
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
        
        # Database configuration - Using your provided credentials
        self.db_config = db_config or {
            'host': 'db4free.net',
            'user': 'lamin_d_kinteh',
            'password': 'Lamin@123',
            'database': 'bdp_report',
            'port': 3306
        }
        self.Transaction = pd.DataFrame()
        self.Onboarding = pd.DataFrame()
        
        st.info(f"Report Period: {self.start_date_overall.strftime('%Y-%m-%d')} to {self.end_date_overall.strftime('%Y-%m-%d')}")
    
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
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10
            )
            return connection
        except pymysql.err.OperationalError as e:
            st.error(f"Database connection error: {e}")
            st.info("Please check: 1) Internet connection 2) Database credentials 3) Database is running")
            return None
        except Exception as e:
            st.error(f"Error connecting to MySQL: {e}")
            return None
    
    def load_data_from_mysql(self, start_date=None, end_date=None):
        """Load data from MySQL database"""
        if start_date is None:
            start_date = self.start_date_overall
        if end_date is None:
            end_date = self.end_date_overall
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            status_text.text("Connecting to database...")
            progress_bar.progress(10)
            
            connection = self.connect_to_mysql()
            if connection is None:
                return False
            
            # Load transaction data
            status_text.text("Loading transaction data from MySQL...")
            progress_bar.progress(30)
            
            transaction_query = """
                SELECT 
                    id, user_identifier, transaction_id, sub_transaction_id,
                    entity_name, full_name, created_by, status, internal_status,
                    service_name, product_name, transaction_type, amount,
                    before_balance, after_balance, ucp_name, wallet_name,
                    pouch_name, reference, error_code, error_message,
                    vendor_transaction_id, vendor_response_code, vendor_message,
                    slug, remarks, created_at, business_hierarchy,
                    parent_user_identifier, parent_full_name
                FROM transactions
                WHERE created_at BETWEEN %s AND %s
                AND created_at IS NOT NULL
                ORDER BY created_at
            """
            
            with connection.cursor() as cursor:
                cursor.execute(transaction_query, (start_date, end_date))
                transaction_results = cursor.fetchall()
                self.transactions = pd.DataFrame(transaction_results)
            
            if len(self.transactions) == 0:
                st.warning("⚠️ No transaction data found for the selected period")
            else:
                st.success(f"✓ Loaded {len(self.transactions)} transaction records")
            
            progress_bar.progress(60)
            
            # Load onboarding data
            status_text.text("Loading onboarding data from MySQL...")
            
            onboarding_query = """
                SELECT 
                    account_id, full_name, mobile, email, region, district,
                    town_village, business_name, kyc_status, registration_date,
                    updated_at, proof_of_id, identification_number,
                    customer_referrer_code, customer_referrer_mobile,
                    referrer_entity, entity, bank, bank_account_name,
                    bank_account_number, status
                FROM onboarding
                WHERE registration_date BETWEEN %s AND %s
                AND registration_date IS NOT NULL
                ORDER BY registration_date
            """
            
            with connection.cursor() as cursor:
                cursor.execute(onboarding_query, (start_date, end_date))
                onboarding_results = cursor.fetchall()
                self.onboarding = pd.DataFrame(onboarding_results)
            
            if len(self.onboarding) == 0:
                st.warning("⚠️ No onboarding data found for the selected period")
            else:
                st.success(f"✓ Loaded {len(self.onboarding)} onboarding records")
            
            progress_bar.progress(90)
            
            # Clean and preprocess data
            status_text.text("Preprocessing data...")
            self._preprocess_data()
            
            progress_bar.progress(100)
            status_text.text("Data loading complete!")
            
            connection.close()
            
            # Display data quality metrics
            self._display_data_quality()
            
            return True
            
        except Exception as e:
            st.error(f"Error loading data from MySQL: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
            return False
    
    def _preprocess_data(self):
        """Preprocess loaded data"""
        if self.transactions.empty and self.onboarding.empty:
            return
        
        # Clean column names
        if not self.transactions.empty:
            self.transactions.columns = self.transactions.columns.str.strip()
        
        if not self.onboarding.empty:
            self.onboarding.columns = self.onboarding.columns.str.strip()
        
        # Parse dates
        if not self.transactions.empty and 'created_at' in self.transactions.columns:
            self.transactions['created_at'] = pd.to_datetime(self.transactions['created_at'], errors='coerce')
        
        if not self.onboarding.empty and 'registration_date' in self.onboarding.columns:
            self.onboarding['registration_date'] = pd.to_datetime(self.onboarding['registration_date'], errors='coerce')
        
        # Clean numeric columns
        if not self.transactions.empty and 'amount' in self.transactions.columns:
            self.transactions['amount'] = pd.to_numeric(self.transactions['amount'], errors='coerce')
        
        # Create User Identifier for merging
        if not self.onboarding.empty and 'mobile' in self.onboarding.columns:
            self.onboarding['user_identifier'] = self.onboarding['mobile'].astype(str).str.strip()
        
        # Clean text columns in transactions
        if not self.transactions.empty:
            text_columns = ['user_identifier', 'product_name', 'entity_name', 'transaction_type', 
                           'ucp_name', 'service_name', 'status', 'sub_transaction_id']
            for col in text_columns:
                if col in self.transactions.columns:
                    self.transactions[col] = self.transactions[col].astype(str).str.strip()
    
    def _display_data_quality(self):
        """Display data quality metrics"""
        with st.expander("📊 Data Quality Summary", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                if not self.transactions.empty:
                    st.metric("Transactions", f"{len(self.transactions):,}")
                    if 'status' in self.transactions.columns:
                        success_rate = (self.transactions['status'] == 'SUCCESS').mean() * 100
                        st.metric("Success Rate", f"{success_rate:.1f}%")
                else:
                    st.metric("Transactions", "0")
                    st.metric("Success Rate", "0%")
            
            with col2:
                if not self.onboarding.empty:
                    st.metric("Onboarding Records", f"{len(self.onboarding):,}")
                    if 'status' in self.onboarding.columns:
                        active_users = (self.onboarding['status'] == 'Active').sum()
                        st.metric("Active Users", f"{active_users:,}")
                else:
                    st.metric("Onboarding Records", "0")
                    st.metric("Active Users", "0")
            
            # Show sample data
            if not self.transactions.empty:
                st.markdown("**Sample Transaction Data:**")
                st.dataframe(self.transactions.head(3), use_container_width=True)
            
            if not self.onboarding.empty:
                st.markdown("**Sample Onboarding Data:**")
                st.dataframe(self.onboarding.head(3), use_container_width=True)
    
    def get_date_filter_options(self):
        """Get date filter options for Streamlit"""
        date_options = {
            'Full Period (Oct 2025 - Jan 2026)': (self.start_date_overall, self.end_date_overall),
            'October 2025': (datetime(2025, 10, 1), datetime(2025, 10, 31)),
            'November 2025': (datetime(2025, 11, 1), datetime(2025, 11, 30)),
            'December 2025': (datetime(2025, 12, 1), datetime(2025, 12, 31)),
            'January 2026 Week 1': (datetime(2026, 1, 1), datetime(2026, 1, 7)),
            'January 2026 Week 2': (datetime(2026, 1, 8), datetime(2026, 1, 14)),
            'Last 7 Days': (self.today - timedelta(days=7), self.today),
            'Last 30 Days': (self.today - timedelta(days=30), self.today),
            'Custom Range': 'custom'
        }
        return date_options
    
    def filter_by_date_range(self, df, date_col, start_date, end_date):
        """Filter dataframe by date range"""
        if df.empty or date_col not in df.columns:
            return pd.DataFrame()
        
        valid_dates = df[date_col].notna()
        mask = (df[date_col] >= start_date) & (df[date_col] <= end_date) & valid_dates
        return df[mask].copy()
    
    def get_new_registered_customers_segmented(self, start_date, end_date):
        """Get new registered customers segmented by Status"""
        if self.onboarding.empty:
            return {'Active': 0, 'Registered': 0, 'TemporaryRegister': 0, 'Total': 0}, {'Active': [], 'Registered': [], 'TemporaryRegister': [], 'Total': []}
        
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
        
        return segmented_counts, customer_lists
    
    def get_active_customers_all(self, start_date, end_date, period_type):
        """Get ALL active customers (not just new ones) based on period type"""
        if self.transactions.empty:
            return [], 0
        
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        
        if not period_transactions.empty:
            # Filter successful customer transactions
            customer_transactions = period_transactions[
                (period_transactions['entity_name'] == 'Customer') &
                (period_transactions['status'] == 'SUCCESS')
            ]
            
            if not customer_transactions.empty:
                # Count transactions per user
                user_transaction_counts = customer_transactions.groupby('user_identifier').size()
                
                # Different thresholds for different period types
                if period_type == 'weekly' or period_type == 'rolling':
                    # Weekly/Rolling: Customers with >=2 transactions
                    threshold = 2
                else:  # monthly
                    # Monthly: Customers with >=10 transactions
                    threshold = 10
                
                active_users = user_transaction_counts[user_transaction_counts >= threshold].index.tolist()
                
                return active_users, len(active_users)
        
        return [], 0
    
    def calculate_executive_snapshot(self, start_date, end_date, period_type):
        """Calculate Executive Snapshot metrics WITH SEGMENTED CUSTOMERS"""
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
        
        for status in ['Active', 'Registered', 'TemporaryRegister']:
            status_customers = segmented_lists[status]
            if status_customers:
                # Get transactions for status customers
                period_transactions = self.filter_by_date_range(
                    self.transactions, 'created_at', start_date, end_date
                )
                
                if not period_transactions.empty:
                    # Filter to status customers' successful transactions
                    status_customer_transactions = period_transactions[
                        (period_transactions['user_identifier'].isin(status_customers)) &
                        (period_transactions['entity_name'] == 'Customer') &
                        (period_transactions['status'] == 'SUCCESS')
                    ]
                    
                    if not status_customer_transactions.empty:
                        # Count transactions per status customer
                        status_customer_counts = status_customer_transactions.groupby('user_identifier').size()
                        
                        # Different thresholds for different period types
                        if period_type in ['weekly', 'rolling']:
                            # Weekly/Rolling: Status customers with >=2 transactions
                            threshold = 2
                        else:  # monthly
                            # Monthly: Status customers with >=10 transactions
                            threshold = 10
                        
                        active_status_customers = status_customer_counts[status_customer_counts >= threshold].index.tolist()
                        wau_by_status[status] = len(active_status_customers)
        
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
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        
        if not period_transactions.empty and 'product_name' in period_transactions.columns:
            # Filter to customer transactions
            customer_transactions = period_transactions[
                (period_transactions['entity_name'] == 'Customer') &
                (period_transactions['status'] == 'SUCCESS') &
                (period_transactions['product_name'].notna())
            ]
            
            if not customer_transactions.empty:
                # For P2P (Internal Wallet Transfer), we need special handling
                product_counts_dict = {}
                product_users_dict = {}
                product_amount_dict = {}
                
                for product in customer_transactions['product_name'].unique():
                    if product == 'Internal Wallet Transfer':
                        # CORRECTED P2P COUNTING:
                        # 1. Only count DR ledger (customer debits)
                        # 2. Exclude fee transactions (UCP Name containing "Fee")
                        p2p_transactions = customer_transactions[
                            (customer_transactions['product_name'] == 'Internal Wallet Transfer') &
                            (customer_transactions['transaction_type'] == 'DR')
                        ]
                        
                        # Exclude fee transactions
                        if 'ucp_name' in p2p_transactions.columns:
                            p2p_transactions = p2p_transactions[
                                ~p2p_transactions['ucp_name'].str.contains('Fee', case=False, na=False)
                            ]
                        
                        product_counts_dict[product] = len(p2p_transactions)
                        product_users_dict[product] = p2p_transactions['user_identifier'].nunique()
                        product_amount_dict[product] = p2p_transactions['amount'].sum() if 'amount' in p2p_transactions.columns else 0
                    else:
                        # For other products, count all transactions
                        product_transactions = customer_transactions[
                            customer_transactions['product_name'] == product
                        ]
                        product_counts_dict[product] = len(product_transactions)
                        product_users_dict[product] = product_transactions['user_identifier'].nunique()
                        product_amount_dict[product] = product_transactions['amount'].sum() if 'amount' in product_transactions.columns else 0
                
                # Also include Airtime Topup as a service
                if 'Airtime Topup' in self.services:
                    airtime_transactions = period_transactions[
                        (period_transactions['service_name'] == 'Airtime Topup') &
                        (period_transactions['entity_name'] == 'Customer') &
                        (period_transactions['status'] == 'SUCCESS') &
                        (period_transactions['transaction_type'] == 'DR')
                    ]
                    product_counts_dict['Airtime Topup'] = len(airtime_transactions)
                    product_users_dict['Airtime Topup'] = airtime_transactions['user_identifier'].nunique()
                    product_amount_dict['Airtime Topup'] = airtime_transactions['amount'].sum() if 'amount' in airtime_transactions.columns else 0
                
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
                    metrics['top_product_users'] = product_users_dict.get(top_product, 0)
                    metrics['top_product_amount'] = product_amount_dict.get(top_product, 0)
                    
                    metrics['low_product'] = low_product
                    metrics['low_product_count'] = low_product_count
                    metrics['low_product_users'] = product_users_dict.get(low_product, 0)
                    metrics['low_product_amount'] = product_amount_dict.get(low_product, 0)
                else:
                    metrics['top_product'] = 'N/A'
                    metrics['low_product'] = 'N/A'
        
        return metrics
    
    def calculate_customer_acquisition(self, start_date, end_date, previous_start=None, previous_end=None):
        """Calculate Customer Acquisition metrics with comparison"""
        metrics = {}
        
        # Filter onboarding for the period
        period_onboarding = self.filter_by_date_range(
            self.onboarding, 'registration_date', start_date, end_date
        )
        
        # Filter transactions for the period
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        
        # Get segmented customer counts
        segmented_counts, segmented_lists = self.get_new_registered_customers_segmented(start_date, end_date)
        
        # New Registrations by Status
        metrics['new_registrations_active'] = segmented_counts['Active']
        metrics['new_registrations_registered'] = segmented_counts['Registered']
        metrics['new_registrations_temporary'] = segmented_counts['TemporaryRegister']
        metrics['new_registrations_total'] = segmented_counts['Total']
        
        # KYC Completed (Status = Active and KYC Status = Verified)
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
        if new_customers_total and not period_transactions.empty:
            # Get successful customer transactions
            customer_transactions = period_transactions[
                (period_transactions['entity_name'] == 'Customer') &
                (period_transactions['status'] == 'SUCCESS')
            ]
            
            if not customer_transactions.empty:
                # Find new customers who transacted
                transacting_new_customers = customer_transactions[
                    customer_transactions['user_identifier'].isin(new_customers_total)
                ]['user_identifier'].unique()
                
                ftt_count = len(transacting_new_customers)
            else:
                ftt_count = 0
        else:
            ftt_count = 0
        metrics['ftt'] = ftt_count
        
        # Previous period comparison if provided
        if previous_start and previous_end:
            prev_segmented_counts, _ = self.get_new_registered_customers_segmented(previous_start, previous_end)
            
            comparison = {}
            for metric in ['new_registrations_total', 'kyc_completed', 'ftt']:
                current = metrics.get(metric, 0) or 0
                previous = prev_segmented_counts.get('Total', 0) if metric == 'new_registrations_total' else 0
                
                if previous > 0:
                    growth = ((current - previous) / previous) * 100
                else:
                    growth = 0 if current > 0 else None
                
                comparison[metric] = {
                    'current': current,
                    'previous': previous,
                    'growth': growth,
                    'trend': '↑' if growth and growth > 5 else '↓' if growth and growth < -5 else '→'
                }
            
            metrics['comparison'] = comparison
        
        return metrics
    
    def calculate_product_usage_performance(self, start_date, end_date, period_type, previous_start=None, previous_end=None):
        """Calculate Product Usage Performance metrics"""
        if self.transactions.empty:
            return {}
        
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        
        product_metrics = {}
        
        # Process regular products
        for category, products in self.product_categories.items():
            for product in products:
                if product == 'Internal Wallet Transfer':
                    # CORRECTED P2P COUNTING
                    product_trans = period_transactions[
                        (period_transactions['product_name'] == 'Internal Wallet Transfer') &
                        (period_transactions['entity_name'] == 'Customer') &
                        (period_transactions['status'] == 'SUCCESS') &
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
                        (period_transactions['product_name'] == product) &
                        (period_transactions['entity_name'] == 'Customer') &
                        (period_transactions['status'] == 'SUCCESS')
                    ]
                
                if not product_trans.empty:
                    # Active Users
                    user_product_counts = product_trans.groupby('user_identifier').size()
                    
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
                
                # Trend if previous period provided
                if previous_start and previous_end:
                    prev_period_transactions = self.filter_by_date_range(
                        self.transactions, 'created_at', previous_start, previous_end
                    )
                    
                    if not prev_period_transactions.empty:
                        if product == 'Internal Wallet Transfer':
                            prev_product_trans = prev_period_transactions[
                                (prev_period_transactions['product_name'] == 'Internal Wallet Transfer') &
                                (prev_period_transactions['entity_name'] == 'Customer') &
                                (prev_period_transactions['status'] == 'SUCCESS') &
                                (prev_period_transactions['transaction_type'] == 'DR')
                            ]
                            
                            if 'ucp_name' in prev_product_trans.columns:
                                prev_product_trans = prev_product_trans[
                                    ~prev_product_trans['ucp_name'].str.contains('Fee', case=False, na=False)
                                ]
                        else:
                            prev_product_trans = prev_period_transactions[
                                (prev_period_transactions['product_name'] == product) &
                                (prev_period_transactions['entity_name'] == 'Customer') &
                                (prev_period_transactions['status'] == 'SUCCESS')
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
                    
                    product_metrics[product]['trend'] = trend
                else:
                    product_metrics[product]['trend'] = '→'
        
        # Process Airtime Topup
        for service in self.services:
            service_trans = period_transactions[
                (period_transactions['service_name'] == service) &
                (period_transactions['entity_name'] == 'Customer') &
                (period_transactions['status'] == 'SUCCESS') &
                (period_transactions['transaction_type'] == 'DR')
            ]
            
            if not service_trans.empty:
                user_service_counts = service_trans.groupby('user_identifier').size()
                
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
        
        return product_metrics
    
    def calculate_customer_activity_engagement(self, start_date, end_date, period_type):
        """Calculate Customer Activity & Engagement metrics"""
        if self.transactions.empty:
            return {
                'wau': 0,
                'avg_transactions_per_user': 0,
                'avg_products_per_user': 0,
                'dormant_users': 0,
                'reactivated_users': 0,
                'total_transactions': 0,
                'period_type': period_type
            }
        
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        
        metrics = {}
        
        if not period_transactions.empty:
            customer_transactions = period_transactions[
                (period_transactions['entity_name'] == 'Customer') &
                (period_transactions['status'] == 'SUCCESS')
            ]
            
            if not customer_transactions.empty:
                wau_active, wau_count = self.get_active_customers_all(start_date, end_date, period_type)
                metrics['wau'] = wau_count
                
                if wau_active:
                    active_user_transactions = customer_transactions[
                        customer_transactions['user_identifier'].isin(wau_active)
                    ]
                    
                    if not active_user_transactions.empty:
                        trans_per_active_user = active_user_transactions.groupby('user_identifier').size()
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
                    'dormant_users': 0,
                    'reactivated_users': 0,
                    'total_transactions': len(customer_transactions),
                    'period_type': period_type
                })
            else:
                metrics = {
                    'wau': 0,
                    'avg_transactions_per_user': 0,
                    'avg_products_per_user': 0,
                    'dormant_users': 0,
                    'reactivated_users': 0,
                    'total_transactions': 0,
                    'period_type': period_type
                }
        else:
            metrics = {
                'wau': 0,
                'avg_transactions_per_user': 0,
                'avg_products_per_user': 0,
                'dormant_users': 0,
                'reactivated_users': 0,
                'total_transactions': 0,
                'period_type': period_type
            }
        
        return metrics

def display_executive_snapshot(metrics, period_name):
    """Display Executive Snapshot metrics"""
    st.markdown(f"<h3 class='sub-header'>{period_name} - Executive Snapshot</h3>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("New Customers (Total)", metrics.get('new_customers_total', 0))
        st.metric("New Customers (Active)", metrics.get('new_customers_active', 0))
    
    with col2:
        st.metric("Active Customers", metrics.get('active_customers_all', 0))
        st.metric("WAU (New Customers)", metrics.get('wau_total', 0))
    
    with col3:
        growth = metrics.get('net_growth_pct', 0)
        growth_display = f"{growth:.1f}%" if growth is not None else "N/A"
        st.metric("Net Growth %", growth_display)
        
        if metrics.get('top_product') != 'N/A':
            st.metric("Top Product", metrics.get('top_product', 'N/A'))
    
    with col4:
        if metrics.get('low_product') != 'N/A':
            st.metric("Lowest Product", metrics.get('low_product', 'N/A'))
            if metrics.get('top_product_count', 0) > 0:
                st.metric("Top Product Usage", metrics.get('top_product_count', 0))
    
    # Display segmented metrics
    with st.expander("📋 Detailed Customer Segmentation", expanded=False):
        seg_col1, seg_col2, seg_col3, seg_col4 = st.columns(4)
        
        with seg_col1:
            st.metric("Active Status", metrics.get('new_customers_active', 0))
            st.metric("WAU Active", metrics.get('wau_active', 0))
        
        with seg_col2:
            st.metric("Registered Status", metrics.get('new_customers_registered', 0))
            st.metric("WAU Registered", metrics.get('wau_registered', 0))
        
        with seg_col3:
            st.metric("Temporary Status", metrics.get('new_customers_temporary', 0))
            st.metric("WAU Temporary", metrics.get('wau_temporary', 0))
        
        with seg_col4:
            st.metric("Total", metrics.get('new_customers_total', 0))
            st.metric("WAU Total", metrics.get('wau_total', 0))

def display_customer_acquisition(metrics):
    """Display Customer Acquisition metrics"""
    st.markdown("<h3 class='sub-header'>👥 Customer Acquisition</h3>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("New Registrations", metrics.get('new_registrations_total', 0))
        st.caption(f"Active: {metrics.get('new_registrations_active', 0)}")
    
    with col2:
        st.metric("KYC Completed", metrics.get('kyc_completed', 0))
        if 'comparison' in metrics and 'kyc_completed' in metrics['comparison']:
            comp = metrics['comparison']['kyc_completed']
            delta = f"{comp['growth']:.1f}%" if comp['growth'] is not None else None
            st.metric("vs Previous", comp['current'], delta=delta)
    
    with col3:
        ftt = metrics.get('ftt', 0)
        total_reg = metrics.get('new_registrations_total', 1)
        ftt_rate = (ftt / total_reg * 100) if total_reg > 0 else 0
        st.metric("First-Time Transactors", ftt)
        st.caption(f"FTT Rate: {ftt_rate:.1f}%")
    
    with col4:
        activation_rate = metrics.get('activation_rate', 0)
        st.metric("Activation Rate", f"{activation_rate:.1f}%" if activation_rate else "N/A")
        st.caption(f"Reactivated: {metrics.get('reactivated_count', 0)}")

def display_product_usage(product_metrics):
    """Display Product Usage Performance"""
    st.markdown("<h3 class='sub-header'>📊 Product Usage Performance</h3>", unsafe_allow_html=True)
    
    if not product_metrics:
        st.info("No product usage data available for this period.")
        return
    
    # Create dataframe for display
    product_data = []
    for product, metrics in product_metrics.items():
        product_data.append({
            'Product': product,
            'Category': metrics['category'],
            'Active Users': metrics['active_users_all'],
            'Transactions': metrics['total_transactions'],
            'Total Amount': f"{metrics['total_amount']:,.2f}",
            'Avg Amount': f"{metrics['avg_amount']:,.2f}",
            'Unique Users': metrics['total_users'],
            'Trend': metrics['trend']
        })
    
    if product_data:
        df = pd.DataFrame(product_data)
        df = df.sort_values('Transactions', ascending=False)
        
        # Display all products
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Product": st.column_config.TextColumn("Product", width="medium"),
                "Category": st.column_config.TextColumn("Category", width="small"),
                "Active Users": st.column_config.NumberColumn("Active Users", format="%d"),
                "Transactions": st.column_config.NumberColumn("Transactions", format="%d"),
                "Total Amount": st.column_config.TextColumn("Total Amount"),
                "Avg Amount": st.column_config.TextColumn("Avg Amount"),
                "Unique Users": st.column_config.NumberColumn("Unique Users", format="%d"),
                "Trend": st.column_config.TextColumn("Trend", width="small")
            }
        )
        
        # Visualization
        col1, col2 = st.columns(2)
        
        with col1:
            # Top products by transactions
            top_products = df.head(10).sort_values('Transactions')
            if not top_products.empty:
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
            if not category_data.empty:
                fig = px.pie(
                    category_data,
                    values='Active Users',
                    names='Category',
                    title='Active Users by Product Category',
                    hole=0.4
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No product usage data available for this period.")

def display_customer_activity(metrics):
    """Display Customer Activity metrics"""
    st.markdown("<h3 class='sub-header'>📱 Customer Activity & Engagement</h3>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Weekly Active Users", metrics.get('wau', 0))
    
    with col2:
        st.metric("Total Transactions", metrics.get('total_transactions', 0))
    
    with col3:
        st.metric("Avg Transactions/User", f"{metrics.get('avg_transactions_per_user', 0):.2f}")
    
    with col4:
        st.metric("Avg Products/User", f"{metrics.get('avg_products_per_user', 0):.2f}")

def create_download_link(df, filename):
    """Create a download link for a dataframe"""
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">📥 Download {filename}</a>'
    return href

def main():
    """Main Streamlit application"""
    # Header
    st.markdown("<h1 class='main-header'>📊 Business Development Performance Dashboard</h1>", unsafe_allow_html=True)
    st.markdown("---")
    
    # Sidebar for configuration
    with st.sidebar:
        st.markdown("### 🔧 Configuration")
        
        # Database configuration with your credentials
        st.markdown("#### Database Connection")
        st.info("Using pre-configured database: db4free.net")
        
        # Show credentials (read-only)
        col1, col2 = st.columns(2)
        with col1:
            st.text_input("Host", "db4free.net", disabled=True)
            st.text_input("Database", "bdp_report", disabled=True)
        with col2:
            st.text_input("User", "lamin_d_kinteh", disabled=True)
            st.text_input("Port", "3306", disabled=True)
        
        # Date range selection
        st.markdown("#### 📅 Date Range")
        
        date_options = {
            'Full Period (Oct 2025 - Jan 2026)': (datetime(2025, 10, 1), min(datetime(2026, 1, 14), datetime.now())),
            'October 2025': (datetime(2025, 10, 1), datetime(2025, 10, 31)),
            'November 2025': (datetime(2025, 11, 1), datetime(2025, 11, 30)),
            'December 2025': (datetime(2025, 12, 1), datetime(2025, 12, 31)),
            'January 2026 Week 1': (datetime(2026, 1, 1), datetime(2026, 1, 7)),
            'January 2026 Week 2': (datetime(2026, 1, 8), min(datetime(2026, 1, 14), datetime.now())),
            'Last 7 Days': (datetime.now() - timedelta(days=7), datetime.now()),
            'Last 30 Days': (datetime.now() - timedelta(days=30), datetime.now()),
            'Custom Range': 'custom'
        }
        
        selected_period = st.selectbox(
            "Select Period",
            list(date_options.keys())
        )
        
        custom_date = st.checkbox("Use Custom Date Range")
        
        if custom_date:
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date", datetime(2025, 10, 1))
            with col2:
                end_date = st.date_input("End Date", min(datetime(2026, 1, 14), datetime.now()))
            start_date = datetime.combine(start_date, datetime.min.time())
            end_date = datetime.combine(end_date, datetime.max.time())
        else:
            start_date, end_date = date_options[selected_period]
        
        # Period type selection
        period_type = st.selectbox(
            "Period Type",
            ['Monthly', 'Weekly', '7-Day Rolling'],
            index=0
        ).lower()
        
        # Load data button
        st.markdown("---")
        load_data = st.button("🚀 Load Data & Generate Report", type="primary", use_container_width=True)
        
        # Quick refresh button
        if st.session_state.get('data_loaded', False):
            if st.button("🔄 Refresh Data", use_container_width=True):
                st.session_state.data_loaded = False
                st.rerun()
        
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
            
            **Current Database:**
            - Host: db4free.net
            - Database: bdp_report
            - User: lamin_d_kinteh
            
            **Period Types:**
            - **Monthly**: ≥10 transactions for active status
            - **Weekly/Rolling**: ≥2 transactions for active status
            """)
    
    # Initialize report generator with your credentials
    db_config = {
        'host': 'db4free.net',
        'user': 'lamin_d_kinteh',
        'password': 'Lamin@123',
        'database': 'bdp_report',
        'port': 3306
    }
    
    generator = PerformanceReportGenerator(db_config)
    
    # Main content area
    if load_data or st.session_state.get('data_loaded', False):
        if load_data:
            st.session_state.data_loaded = True
        
        with st.spinner("Loading data from MySQL..."):
            success = generator.load_data_from_mysql(start_date, end_date)
        
        if success and (not generator.transactions.empty or not generator.onboarding.empty):
            # Calculate metrics
            with st.spinner("Calculating metrics..."):
                # Executive Snapshot
                exec_metrics = generator.calculate_executive_snapshot(start_date, end_date, period_type)
                
                # Customer Acquisition
                cust_acq_metrics = generator.calculate_customer_acquisition(start_date, end_date)
                
                # Product Usage
                product_metrics = generator.calculate_product_usage_performance(start_date, end_date, period_type)
                
                # Customer Activity
                activity_metrics = generator.calculate_customer_activity_engagement(start_date, end_date, period_type)
            
            # Display metrics in tabs
            tab1, tab2, tab3, tab4, tab5 = st.tabs([
                "📈 Executive Snapshot", 
                "👥 Customer Acquisition", 
                "📊 Product Usage", 
                "📱 Customer Activity",
                "📥 Export Data"
            ])
            
            with tab1:
                display_executive_snapshot(exec_metrics, selected_period)
            
            with tab2:
                display_customer_acquisition(cust_acq_metrics)
            
            with tab3:
                display_product_usage(product_metrics)
            
            with tab4:
                display_customer_activity(activity_metrics)
            
            with tab5:
                st.markdown("<h3 class='sub-header'>📥 Export Data</h3>", unsafe_allow_html=True)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Export transactions
                    if not generator.transactions.empty:
                        csv = generator.transactions.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Transactions CSV",
                            data=csv,
                            file_name=f"transactions_{start_date.date()}_to_{end_date.date()}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                    else:
                        st.warning("No transaction data to export")
                
                with col2:
                    # Export onboarding
                    if not generator.onboarding.empty:
                        csv = generator.onboarding.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Onboarding CSV",
                            data=csv,
                            file_name=f"onboarding_{start_date.date()}_to_{end_date.date()}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                    else:
                        st.warning("No onboarding data to export")
                
                # Export summary report
                st.markdown("---")
                st.markdown("### 📊 Export Summary Report")
                
                summary_data = {
                    'Metric': [
                        'Report Period',
                        'Start Date',
                        'End Date',
                        'Period Type',
                        'New Customers Total',
                        'Active Customers',
                        'WAU',
                        'New Registrations',
                        'KYC Completed',
                        'First-Time Transactors',
                        'Total Transactions',
                        'Top Product',
                        'Top Product Transactions'
                    ],
                    'Value': [
                        selected_period,
                        start_date.strftime('%Y-%m-%d'),
                        end_date.strftime('%Y-%m-%d'),
                        period_type,
                        exec_metrics.get('new_customers_total', 0),
                        exec_metrics.get('active_customers_all', 0),
                        activity_metrics.get('wau', 0),
                        cust_acq_metrics.get('new_registrations_total', 0),
                        cust_acq_metrics.get('kyc_completed', 0),
                        cust_acq_metrics.get('ftt', 0),
                        activity_metrics.get('total_transactions', 0),
                        exec_metrics.get('top_product', 'N/A'),
                        exec_metrics.get('top_product_count', 0)
                    ]
                }
                
                summary_df = pd.DataFrame(summary_data)
                csv = summary_df.to_csv(index=False)
                
                st.download_button(
                    label="📥 Download Summary Report",
                    data=csv,
                    file_name=f"performance_summary_{start_date.date()}_to_{end_date.date()}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
                
                # Show SQL queries
                with st.expander("🔍 View SQL Queries", expanded=False):
                    st.code(f"""
                    -- Transactions Query
                    SELECT * FROM transactions 
                    WHERE created_at BETWEEN '{start_date}' AND '{end_date}'
                    ORDER BY created_at;
                    
                    -- Onboarding Query  
                    SELECT * FROM onboarding
                    WHERE registration_date BETWEEN '{start_date}' AND '{end_date}'
                    ORDER BY registration_date;
                    """, language="sql")
        else:
            st.error("No data found or failed to load data. Please check:")
            st.info("""
            1. Database tables exist (transactions, onboarding)
            2. Tables have data for selected date range
            3. Internet connection is working
            4. Database is accessible from your location
            """)
    
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
        
        ### 🔧 Current Configuration
        
        **Database:** db4free.net  
        **Database Name:** bdp_report  
        **User:** lamin_d_kinteh
        
        ### ⚙️ Requirements
        
        Make sure your MySQL database has the following tables:
        
        **transactions table** with columns:
        - `id`, `user_identifier`, `created_at`, `status`, `product_name`, `service_name`, `amount`, `entity_name`, `transaction_type`, `ucp_name`
        
        **onboarding table** with columns:
        - `account_id`, `mobile`, `registration_date`, `kyc_status`, `status`, `entity`
        """)
        
        # Quick start buttons
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("📅 Last 30 Days", use_container_width=True):
                st.session_state.quick_start = "last_30_days"
                st.rerun()
        with col2:
            if st.button("📆 Current Month", use_container_width=True):
                st.session_state.quick_start = "current_month"
                st.rerun()
        with col3:
            if st.button("📊 Full Period", use_container_width=True):
                st.session_state.quick_start = "full_period"
                st.rerun()

if __name__ == "__main__":
    main()
