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
import time

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
        border: 1px solid #E5E7EB;
    }
    .stButton button {
        width: 100%;
    }
    .filter-section {
        background-color: #F8FAFC;
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 1rem;
        border: 1px solid #E5E7EB;
    }
    .success-box {
        background-color: #D1FAE5;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #10B981;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #FEE2E2;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #EF4444;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Database configuration (hardcoded)
DB_CONFIG = {
    'host': 'db4free.net',
    'user': 'lamin_d_kinteh',
    'password': 'Lamin@123',
    'database': 'bdp_report',
    'port': 3306
}

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
        
        self.transactions = pd.DataFrame()
        self.onboarding = pd.DataFrame()
        
        # Store loaded data with timestamps for caching
        self.data_cache = {}
    
    def connect_to_mysql(self):
        """Connect to MySQL database"""
        try:
            connection = pymysql.connect(
                host=DB_CONFIG['host'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                database=DB_CONFIG['database'],
                port=DB_CONFIG.get('port', 3306),
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10
            )
            return connection
        except Exception as e:
            st.error(f"❌ Error connecting to MySQL: {str(e)}")
            return None
    
    def test_connection(self):
        """Test database connection and table structure"""
        try:
            connection = self.connect_to_mysql()
            if connection is None:
                return False
            
            with connection.cursor() as cursor:
                # Test Transaction table
                cursor.execute("SHOW COLUMNS FROM Transaction")
                transaction_columns = [col['Field'] for col in cursor.fetchall()]
                st.info(f"📊 Transaction table columns: {', '.join(transaction_columns[:10])}...")
                
                # Test Onboarding table
                cursor.execute("SHOW COLUMNS FROM Onboarding")
                onboarding_columns = [col['Field'] for col in cursor.fetchall()]
                st.info(f"👥 Onboarding table columns: {', '.join(onboarding_columns[:10])}...")
                
                # Count records
                cursor.execute("SELECT COUNT(*) as count FROM Transaction WHERE created_at BETWEEN '2025-12-01' AND '2025-12-31'")
                trans_count = cursor.fetchone()['count']
                
                cursor.execute("SELECT COUNT(*) as count FROM Onboarding WHERE registration_date BETWEEN '2025-12-01' AND '2025-12-31'")
                onboard_count = cursor.fetchone()['count']
                
                st.success(f"✅ Found {trans_count} transactions and {onboard_count} onboarding records for December 2025")
            
            connection.close()
            return True
            
        except Exception as e:
            st.error(f"❌ Database test failed: {str(e)}")
            return False
    
    def load_data_from_mysql(self, start_date=None, end_date=None, force_reload=False):
        """Load data from MySQL database with caching"""
        if start_date is None:
            start_date = self.start_date_overall
        if end_date is None:
            end_date = self.end_date_overall
        
        # Create cache key
        cache_key = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
        
        # Check cache if not forcing reload
        if not force_reload and cache_key in self.data_cache:
            cache_data = self.data_cache[cache_key]
            if time.time() - cache_data['timestamp'] < 300:  # 5 minute cache
                self.transactions = cache_data['transactions']
                self.onboarding = cache_data['onboarding']
                st.success(f"✅ Using cached data (loaded {int(time.time() - cache_data['timestamp'])} seconds ago)")
                return True
        
        # Show date range
        st.info(f"📅 Loading data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Test connection first
        if not self.test_connection():
            return False
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            connection = self.connect_to_mysql()
            if connection is None:
                return False
            
            # Load transaction data - using correct column names from your sample
            status_text.text("📥 Loading transaction data from MySQL...")
            progress_bar.progress(20)
            
            transaction_query = """
                SELECT 
                    user_identifier, 
                    entity_name, 
                    status, 
                    service_name, 
                    product_name, 
                    transaction_type, 
                    amount, 
                    ucp_name, 
                    created_at,
                    transaction_id,
                    sub_transaction_id
                FROM Transaction
                WHERE created_at BETWEEN %s AND %s
                AND status = 'SUCCESS'
            """
            
            with connection.cursor() as cursor:
                cursor.execute(transaction_query, (start_date, end_date))
                transaction_results = cursor.fetchall()
                
                if transaction_results:
                    self.transactions = pd.DataFrame(transaction_results)
                    st.success(f"✅ Loaded {len(self.transactions)} transaction records")
                else:
                    self.transactions = pd.DataFrame()
                    st.warning("⚠️ No transaction records found for selected period")
            
            progress_bar.progress(50)
            
            # Load onboarding data - using correct column names from your sample
            status_text.text("📥 Loading onboarding data from MySQL...")
            
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
                WHERE registration_date BETWEEN %s AND %s
            """
            
            with connection.cursor() as cursor:
                cursor.execute(onboarding_query, (start_date, end_date))
                onboarding_results = cursor.fetchall()
                
                if onboarding_results:
                    self.onboarding = pd.DataFrame(onboarding_results)
                    st.success(f"✅ Loaded {len(self.onboarding)} onboarding records")
                else:
                    self.onboarding = pd.DataFrame()
                    st.warning("⚠️ No onboarding records found for selected period")
            
            progress_bar.progress(80)
            
            # Clean and preprocess data
            status_text.text("🧹 Preprocessing data...")
            self._preprocess_data()
            
            progress_bar.progress(100)
            status_text.text("✅ Data loading complete!")
            
            connection.close()
            
            # Cache the data if we have any
            if len(self.transactions) > 0 or len(self.onboarding) > 0:
                self.data_cache[cache_key] = {
                    'transactions': self.transactions.copy(),
                    'onboarding': self.onboarding.copy(),
                    'timestamp': time.time()
                }
            
            return True
            
        except Exception as e:
            st.error(f"❌ Error loading data from MySQL: {str(e)}")
            return False
    
    def _preprocess_data(self):
        """Preprocess loaded data"""
        try:
            # Parse dates
            if 'created_at' in self.transactions.columns and len(self.transactions) > 0:
                self.transactions['created_at'] = pd.to_datetime(self.transactions['created_at'], errors='coerce')
            
            if 'registration_date' in self.onboarding.columns and len(self.onboarding) > 0:
                self.onboarding['registration_date'] = pd.to_datetime(self.onboarding['registration_date'], errors='coerce')
            
            # Clean numeric columns
            if 'amount' in self.transactions.columns and len(self.transactions) > 0:
                self.transactions['amount'] = pd.to_numeric(self.transactions['amount'], errors='coerce')
            
            # Create consistent user identifier for merging
            # For transactions, use user_identifier directly
            if len(self.transactions) > 0:
                if 'user_identifier' in self.transactions.columns:
                    self.transactions['user_id'] = self.transactions['user_identifier'].astype(str).str.strip()
                else:
                    # If no user_identifier, create a dummy one
                    self.transactions['user_id'] = 'unknown'
            
            # For onboarding, use mobile as user identifier
            if len(self.onboarding) > 0:
                if 'mobile' in self.onboarding.columns:
                    self.onboarding['user_id'] = self.onboarding['mobile'].astype(str).str.strip()
                else:
                    self.onboarding['user_id'] = 'unknown'
            
            # Clean text columns
            text_columns = ['user_id', 'product_name', 'entity_name', 'transaction_type', 
                           'ucp_name', 'service_name', 'status']
            for col in text_columns:
                if col in self.transactions.columns and len(self.transactions) > 0:
                    self.transactions[col] = self.transactions[col].astype(str).str.strip()
            
            # Clean onboarding text columns
            onboard_text_cols = ['user_id', 'kyc_status', 'status', 'entity']
            for col in onboard_text_cols:
                if col in self.onboarding.columns and len(self.onboarding) > 0:
                    self.onboarding[col] = self.onboarding[col].astype(str).str.strip()
            
            st.success("✅ Data preprocessing complete")
            
        except Exception as e:
            st.warning(f"⚠️ Some preprocessing issues: {str(e)}")
    
    def _display_data_summary(self):
        """Display data summary"""
        with st.expander("📊 Data Summary", expanded=True):
            col1, col2 = st.columns(2)
            
            with col1:
                if len(self.transactions) > 0:
                    st.metric("Transactions", f"{len(self.transactions):,}")
                    if 'amount' in self.transactions.columns:
                        total_amount = self.transactions['amount'].sum()
                        st.metric("Total Amount", f"GMD {total_amount:,.2f}")
                    if 'status' in self.transactions.columns:
                        success_rate = (self.transactions['status'] == 'SUCCESS').mean() * 100
                        st.metric("Success Rate", f"{success_rate:.1f}%")
                else:
                    st.warning("No transaction data")
            
            with col2:
                if len(self.onboarding) > 0:
                    st.metric("Onboarding Records", f"{len(self.onboarding):,}")
                    if 'status' in self.onboarding.columns:
                        active_users = (self.onboarding['status'] == 'Active').sum()
                        st.metric("Active Users", f"{active_users:,}")
                    if 'kyc_status' in self.onboarding.columns:
                        verified_users = (self.onboarding['kyc_status'].str.upper() == 'VERIFIED').sum()
                        st.metric("KYC Verified", f"{verified_users:,}")
                else:
                    st.warning("No onboarding data")
    
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
        
        if not period_onboarding.empty:
            # Filter customers with status in ['Active', 'Registered', 'TemporaryRegister']
            valid_statuses = ['Active', 'Registered', 'TemporaryRegister']
            valid_customers = period_onboarding[
                period_onboarding['status'].isin(valid_statuses)
            ]
            
            # Segment by status
            for status in valid_statuses:
                status_customers = valid_customers[valid_customers['status'] == status]
                segmented_counts[status] = status_customers['user_id'].nunique()
                customer_lists[status] = status_customers['user_id'].unique().tolist()
            
            # Total
            segmented_counts['Total'] = valid_customers['user_id'].nunique()
            customer_lists['Total'] = valid_customers['user_id'].unique().tolist()
        
        return segmented_counts, customer_lists
    
    def get_active_customers_all(self, start_date, end_date, period_type):
        """Get ALL active customers based on period type"""
        if self.transactions.empty:
            return [], 0
        
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        
        if not period_transactions.empty:
            # Filter successful customer transactions
            customer_transactions = period_transactions[
                period_transactions['entity_name'] == 'Customer'
            ]
            
            if not customer_transactions.empty:
                # Count transactions per user
                user_transaction_counts = customer_transactions.groupby('user_id').size()
                
                # Different thresholds for different period types
                if period_type == 'weekly' or period_type == 'rolling':
                    threshold = 2
                else:  # monthly
                    threshold = 10
                
                active_users = user_transaction_counts[user_transaction_counts >= threshold].index.tolist()
                
                return active_users, len(active_users)
        
        return [], 0
    
    def calculate_executive_snapshot(self, start_date, end_date, period_type):
        """Calculate Executive Snapshot metrics"""
        metrics = {}
        
        # Get new registered customers SEGMENTED BY STATUS
        segmented_counts, segmented_lists = self.get_new_registered_customers_segmented(start_date, end_date)
        
        metrics['new_customers_active'] = segmented_counts['Active']
        metrics['new_customers_registered'] = segmented_counts['Registered']
        metrics['new_customers_temporary'] = segmented_counts['TemporaryRegister']
        metrics['new_customers_total'] = segmented_counts['Total']
        
        # Get ALL active customers
        active_customers_all, active_count_all = self.get_active_customers_all(start_date, end_date, period_type)
        metrics['active_customers_all'] = active_count_all
        
        # Weekly Active Users (WAU) from new registered customers
        wau_by_status = {'Active': 0, 'Registered': 0, 'TemporaryRegister': 0, 'Total': 0}
        
        for status in ['Active', 'Registered', 'TemporaryRegister']:
            status_customers = segmented_lists[status]
            if status_customers and not self.transactions.empty:
                # Get transactions for status customers
                period_transactions = self.filter_by_date_range(
                    self.transactions, 'created_at', start_date, end_date
                )
                
                if not period_transactions.empty:
                    # Filter to status customers' successful transactions
                    status_customer_transactions = period_transactions[
                        period_transactions['user_id'].isin(status_customers)
                    ]
                    
                    if not status_customer_transactions.empty:
                        # Count transactions per status customer
                        status_customer_counts = status_customer_transactions.groupby('user_id').size()
                        
                        # Different thresholds for different period types
                        if period_type in ['weekly', 'rolling']:
                            threshold = 2
                        else:  # monthly
                            threshold = 10
                        
                        active_status_customers = status_customer_counts[status_customer_counts >= threshold].index.tolist()
                        wau_by_status[status] = len(active_status_customers)
        
        metrics['wau_active'] = wau_by_status['Active']
        metrics['wau_registered'] = wau_by_status['Registered']
        metrics['wau_temporary'] = wau_by_status['TemporaryRegister']
        metrics['wau_total'] = sum(wau_by_status.values())
        
        # Net Customer Growth
        try:
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
        except:
            metrics['net_growth_pct'] = None
        
        # Top and Lowest Performing Products
        if not self.transactions.empty:
            period_transactions = self.filter_by_date_range(
                self.transactions, 'created_at', start_date, end_date
            )
            
            if not period_transactions.empty and 'product_name' in period_transactions.columns:
                product_counts_dict = {}
                product_users_dict = {}
                product_amount_dict = {}
                
                # Process each product
                for product in period_transactions['product_name'].unique():
                    if pd.isna(product):
                        continue
                    
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
                        product_users_dict[product] = p2p_transactions['user_id'].nunique()
                        product_amount_dict[product] = p2p_transactions['amount'].sum() if 'amount' in p2p_transactions.columns else 0
                    else:
                        # For other products
                        product_transactions = period_transactions[
                            period_transactions['product_name'] == product
                        ]
                        product_counts_dict[product] = len(product_transactions)
                        product_users_dict[product] = product_transactions['user_id'].nunique()
                        product_amount_dict[product] = product_transactions['amount'].sum() if 'amount' in product_transactions.columns else 0
                
                # Also include Airtime Topup as a service
                if 'Airtime Topup' in self.services:
                    airtime_transactions = period_transactions[
                        (period_transactions['service_name'] == 'Airtime Topup') &
                        (period_transactions['transaction_type'] == 'DR')
                    ]
                    product_counts_dict['Airtime Topup'] = len(airtime_transactions)
                    product_users_dict['Airtime Topup'] = airtime_transactions['user_id'].nunique()
                    product_amount_dict['Airtime Topup'] = airtime_transactions['amount'].sum() if 'amount' in airtime_transactions.columns else 0
                
                # Convert to Series for sorting
                if product_counts_dict:
                    product_counts = pd.Series(product_counts_dict)
                    product_counts = product_counts.sort_values(ascending=False)
                    
                    # Get top performing product
                    if len(product_counts) > 0:
                        top_product = product_counts.index[0]
                        top_product_count = int(product_counts.iloc[0])
                        
                        # Get lowest performing product (with at least 1 transaction)
                        active_products = product_counts[product_counts > 0]
                        if len(active_products) > 0:
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
                else:
                    metrics['top_product'] = 'N/A'
                    metrics['low_product'] = 'N/A'
        
        return metrics
    
    def calculate_customer_acquisition(self, start_date, end_date):
        """Calculate Customer Acquisition metrics"""
        metrics = {}
        
        # Get segmented customer counts
        segmented_counts, segmented_lists = self.get_new_registered_customers_segmented(start_date, end_date)
        
        # New Registrations by Status
        metrics['new_registrations_active'] = segmented_counts['Active']
        metrics['new_registrations_registered'] = segmented_counts['Registered']
        metrics['new_registrations_temporary'] = segmented_counts['TemporaryRegister']
        metrics['new_registrations_total'] = segmented_counts['Total']
        
        # KYC Completed (Status = Active and KYC Status = Verified)
        if not self.onboarding.empty:
            period_onboarding = self.filter_by_date_range(
                self.onboarding, 'registration_date', start_date, end_date
            )
            
            if not period_onboarding.empty and 'kyc_status' in period_onboarding.columns:
                kyc_completed = period_onboarding[
                    (period_onboarding['kyc_status'].str.upper() == 'VERIFIED') &
                    (period_onboarding['status'] == 'Active')
                ]['user_id'].nunique()
            else:
                kyc_completed = 0
        else:
            kyc_completed = 0
        metrics['kyc_completed'] = kyc_completed
        
        # First-Time Transactors (FTT) - New registered customers who transacted
        new_customers_total = segmented_lists['Total']
        if new_customers_total and not self.transactions.empty:
            period_transactions = self.filter_by_date_range(
                self.transactions, 'created_at', start_date, end_date
            )
            
            if not period_transactions.empty:
                # Find new customers who transacted
                transacting_new_customers = period_transactions[
                    period_transactions['user_id'].isin(new_customers_total)
                ]['user_id'].unique()
                
                ftt_count = len(transacting_new_customers)
            else:
                ftt_count = 0
        else:
            ftt_count = 0
        metrics['ftt'] = ftt_count
        
        return metrics
    
    def calculate_product_usage_performance(self, start_date, end_date, period_type):
        """Calculate Product Usage Performance metrics"""
        if self.transactions.empty:
            return {}
        
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        
        if period_transactions.empty:
            return {}
        
        product_metrics = {}
        
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
                    user_product_counts = product_trans.groupby('user_id').size()
                    
                    # Different thresholds for different period types
                    if period_type in ['weekly', 'rolling']:
                        threshold = 2
                    else:  # monthly
                        threshold = 10
                    
                    active_users_all = (user_product_counts >= threshold).sum()
                    
                    # Total metrics
                    total_transactions = len(product_trans)
                    total_amount = product_trans['amount'].sum() if 'amount' in product_trans.columns else 0
                    total_users = product_trans['user_id'].nunique()
                    avg_amount = total_amount / total_transactions if total_transactions > 0 else 0
                    
                    product_metrics[product] = {
                        'category': category,
                        'active_users_all': int(active_users_all),
                        'total_transactions': int(total_transactions),
                        'total_amount': float(total_amount),
                        'avg_amount': float(avg_amount),
                        'total_users': int(total_users)
                    }
        
        # Process Airtime Topup
        for service in self.services:
            service_trans = period_transactions[
                (period_transactions['service_name'] == service) &
                (period_transactions['transaction_type'] == 'DR')
            ]
            
            if not service_trans.empty:
                user_service_counts = service_trans.groupby('user_id').size()
                
                if period_type in ['weekly', 'rolling']:
                    threshold = 2
                else:  # monthly
                    threshold = 10
                    
                active_users_all = (user_service_counts >= threshold).sum()
                
                total_transactions = len(service_trans)
                total_amount = service_trans['amount'].sum() if 'amount' in service_trans.columns else 0
                total_users = service_trans['user_id'].nunique()
                avg_amount = total_amount / total_transactions if total_transactions > 0 else 0
                
                product_metrics[service] = {
                    'category': 'Airtime Topup',
                    'active_users_all': int(active_users_all),
                    'total_transactions': int(total_transactions),
                    'total_amount': float(total_amount),
                    'avg_amount': float(avg_amount),
                    'total_users': int(total_users)
                }
        
        return product_metrics
    
    def calculate_customer_activity_engagement(self, start_date, end_date, period_type):
        """Calculate Customer Activity & Engagement metrics"""
        if self.transactions.empty:
            return {
                'wau': 0,
                'avg_transactions_per_user': 0.0,
                'avg_products_per_user': 0.0,
                'total_transactions': 0
            }
        
        period_transactions = self.filter_by_date_range(
            self.transactions, 'created_at', start_date, end_date
        )
        
        if period_transactions.empty:
            return {
                'wau': 0,
                'avg_transactions_per_user': 0.0,
                'avg_products_per_user': 0.0,
                'total_transactions': 0
            }
        
        metrics = {}
        
        # Get active customers
        wau_active, wau_count = self.get_active_customers_all(start_date, end_date, period_type)
        metrics['wau'] = int(wau_count)
        
        if wau_active:
            active_user_transactions = period_transactions[
                period_transactions['user_id'].isin(wau_active)
            ]
            
            if not active_user_transactions.empty:
                trans_per_active_user = active_user_transactions.groupby('user_id').size()
                avg_transactions_per_user = float(trans_per_active_user.mean())
                
                products_per_active_user = active_user_transactions.groupby('user_id')['product_name'].nunique()
                avg_products_per_user = float(products_per_active_user.mean())
            else:
                avg_transactions_per_user = 0.0
                avg_products_per_user = 0.0
        else:
            avg_transactions_per_user = 0.0
            avg_products_per_user = 0.0
        
        metrics.update({
            'avg_transactions_per_user': avg_transactions_per_user,
            'avg_products_per_user': avg_products_per_user,
            'total_transactions': int(len(period_transactions))
        })
        
        return metrics

# Display functions
def display_executive_snapshot(metrics, period_name):
    """Display Executive Snapshot metrics"""
    st.markdown(f"<h3 class='sub-header'>📈 Executive Snapshot - {period_name}</h3>", unsafe_allow_html=True)
    
    # Row 1: Main Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_new = metrics.get('new_customers_total', 0)
        st.metric("New Customers (Total)", total_new)
        if total_new > 0:
            with st.expander("📊 Segmented View"):
                st.metric("Active Status", metrics.get('new_customers_active', 0))
                st.metric("Registered Status", metrics.get('new_customers_registered', 0))
                st.metric("Temporary Status", metrics.get('new_customers_temporary', 0))
    
    with col2:
        active_customers = metrics.get('active_customers_all', 0)
        st.metric("Active Customers", active_customers)
        growth = metrics.get('net_growth_pct', 0)
        if growth is not None:
            growth_display = f"{growth:.1f}%"
            delta = f"{growth:+.1f}%" if growth != 0 else None
            st.metric("Net Growth %", growth_display, delta=delta)
        else:
            st.metric("Net Growth %", "N/A")
    
    with col3:
        wau_total = metrics.get('wau_total', 0)
        st.metric("WAU (New Customers)", wau_total)
        if wau_total > 0:
            with st.expander("👥 WAU by Status"):
                st.metric("Active WAU", metrics.get('wau_active', 0))
                st.metric("Registered WAU", metrics.get('wau_registered', 0))
                st.metric("Temporary WAU", metrics.get('wau_temporary', 0))
    
    with col4:
        top_product = metrics.get('top_product', 'N/A')
        if top_product != 'N/A':
            st.metric("Top Product", top_product)
            st.metric("Transactions", metrics.get('top_product_count', 0))
        else:
            st.info("No product data available")

def display_customer_acquisition(metrics):
    """Display Customer Acquisition metrics"""
    st.markdown("<h3 class='sub-header'>👥 Customer Acquisition</h3>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_reg = metrics.get('new_registrations_total', 0)
        st.metric("New Registrations", total_reg)
        if total_reg > 0:
            with st.expander("📋 By Status"):
                st.metric("Active", metrics.get('new_registrations_active', 0))
                st.metric("Registered", metrics.get('new_registrations_registered', 0))
                st.metric("Temporary", metrics.get('new_registrations_temporary', 0))
    
    with col2:
        kyc_completed = metrics.get('kyc_completed', 0)
        st.metric("KYC Completed", kyc_completed)
        if total_reg > 0:
            kyc_rate = (kyc_completed / total_reg * 100)
            st.caption(f"📈 KYC Rate: {kyc_rate:.1f}%")
    
    with col3:
        ftt = metrics.get('ftt', 0)
        st.metric("First-Time Transactors", ftt)
        if total_reg > 0:
            ftt_rate = (ftt / total_reg * 100)
            st.caption(f"📈 FTT Rate: {ftt_rate:.1f}%")
    
    with col4:
        # Calculate activation rate if we have data
        if total_reg > 0 and ftt > 0:
            activation_rate = (ftt / total_reg * 100)
            st.metric("Activation Rate", f"{activation_rate:.1f}%")
        else:
            st.metric("Activation Rate", "N/A")

def display_product_usage(product_metrics):
    """Display Product Usage Performance"""
    st.markdown("<h3 class='sub-header'>📊 Product Usage Performance</h3>", unsafe_allow_html=True)
    
    if not product_metrics:
        st.info("📭 No product usage data available for this period.")
        return
    
    # Create dataframe for display
    product_data = []
    for product, metrics in product_metrics.items():
        if metrics['total_transactions'] > 0:  # Only show products with transactions
            product_data.append({
                'Product': str(product),
                'Category': str(metrics['category']),
                'Active Users': int(metrics['active_users_all']),
                'Transactions': int(metrics['total_transactions']),
                'Total Amount': float(metrics['total_amount']),
                'Avg Amount': float(metrics['avg_amount']),
                'Unique Users': int(metrics['total_users'])
            })
    
    if product_data:
        df = pd.DataFrame(product_data)
        df = df.sort_values('Transactions', ascending=False)
        
        # Display top products in a table
        st.dataframe(
            df,
            hide_index=True,
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
                top_products = df.head(10).sort_values('Transactions', ascending=True)
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
    else:
        st.info("📭 No product usage data available for this period.")

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

def main():
    """Main Streamlit application"""
    # Header
    st.markdown("<h1 class='main-header'>📊 Business Development Performance Dashboard</h1>", unsafe_allow_html=True)
    st.markdown("---")
    
    # Initialize session state for caching
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    if 'current_filters' not in st.session_state:
        st.session_state.current_filters = {}
    
    # Sidebar for filters
    with st.sidebar:
        st.markdown("### ⚡ Quick Filters")
        
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
            'Last 30 Days': (datetime.now() - timedelta(days=30), datetime.now())
        }
        
        selected_period = st.selectbox(
            "Select Period",
            list(date_options.keys()),
            key="period_selector"
        )
        
        # Custom date range option
        use_custom = st.checkbox("Custom Date Range", key="custom_date_check")
        
        if use_custom:
            col1, col2 = st.columns(2)
            with col1:
                start_date_input = st.date_input(
                    "Start Date", 
                    datetime(2025, 10, 1),
                    key="custom_start_date"
                )
            with col2:
                end_date_input = st.date_input(
                    "End Date", 
                    min(datetime(2026, 1, 14), datetime.now()),
                    key="custom_end_date"
                )
            start_date = datetime.combine(start_date_input, datetime.min.time())
            end_date = datetime.combine(end_date_input, datetime.max.time())
        else:
            start_date, end_date = date_options[selected_period]
        
        # Period type selection
        st.markdown("#### ⏰ Period Type")
        period_type = st.selectbox(
            "Select Analysis Period Type",
            ['Monthly', 'Weekly', '7-Day Rolling'],
            index=0,
            key="period_type_selector"
        ).lower()
        
        # Action buttons
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            load_button = st.button("🚀 Load Data", type="primary", use_container_width=True)
        
        with col2:
            refresh_button = st.button("🔄 Refresh", use_container_width=True)
        
        # Database info
        st.markdown("---")
        with st.expander("🔒 Connection Info"):
            st.info(f"""
            **Connected to:** {DB_CONFIG['host']}  
            **Database:** {DB_CONFIG['database']}  
            **Tables:** Transaction, Onboarding  
            """)
        
        # Info section
        st.markdown("---")
        with st.expander("ℹ️ About"):
            st.markdown("""
            **Active Customer Definitions:**
            - **Monthly**: ≥10 transactions in month
            - **Weekly/Rolling**: ≥2 transactions in period
            
            **Key Metrics:**
            - WAU: Weekly Active Users
            - FTT: First-Time Transactors
            - KYC Rate: Verified customers
            - Net Growth: Customer growth vs previous period
            """)
    
    # Main content
    if load_button or refresh_button or st.session_state.data_loaded:
        # Check if filters changed
        current_filters = {
            'start_date': start_date,
            'end_date': end_date,
            'period_type': period_type,
            'use_custom': use_custom
        }
        
        # Force reload if refresh button clicked or filters changed
        force_reload = refresh_button or (current_filters != st.session_state.get('current_filters', {}))
        
        # Update session state
        st.session_state.current_filters = current_filters
        
        # Initialize generator
        generator = PerformanceReportGenerator()
        
        # Load data
        success = generator.load_data_from_mysql(start_date, end_date, force_reload)
        
        if success:
            st.session_state.data_loaded = True
            
            # Display data summary
            generator._display_data_summary()
            
            # Only calculate metrics if we have data
            if len(generator.transactions) > 0 or len(generator.onboarding) > 0:
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
                        if not generator.transactions.empty:
                            csv = generator.transactions.to_csv(index=False)
                            st.download_button(
                                label="📥 Download Transactions CSV",
                                data=csv,
                                file_name=f"transactions_{start_date.date()}_to_{end_date.date()}.csv",
                                mime="text/csv"
                            )
                    
                    with col2:
                        if not generator.onboarding.empty:
                            csv = generator.onboarding.to_csv(index=False)
                            st.download_button(
                                label="📥 Download Onboarding CSV",
                                data=csv,
                                file_name=f"onboarding_{start_date.date()}_to_{end_date.date()}.csv",
                                mime="text/csv"
                            )
            else:
                st.warning("⚠️ No data available for the selected period. Try a different date range.")
        else:
            st.error("❌ Failed to load data. Please check your connection and try again.")
    else:
        # Welcome message
        st.markdown("""
        ## Welcome to the Business Development Performance Dashboard!
        
        ### 🚀 Quick Start
        
        1. **Select Date Range** in the sidebar
        2. **Choose Period Type** for analysis
        3. Click **"Load Data"** to begin
        
        ### 📊 Available Reports
        
        - **Executive Snapshot**: Key performance indicators
        - **Customer Acquisition**: Registration metrics
        - **Product Usage**: Product performance analysis
        - **Customer Activity**: Engagement metrics
        - **Data Export**: Download reports
        
        ### ⚡ Performance Optimizations
        
        - **Caching**: Data is cached for 5 minutes
        - **Optimized Queries**: Faster data loading
        - **Smart Filtering**: Real-time updates
        
        *Ready to begin? Configure your filters and click "Load Data"!*
        """)
        
        # Quick stats
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Report Coverage", "Oct 2025 - Jan 2026")
        with col2:
            st.metric("Database Tables", "2")
        with col3:
            st.metric("Analysis Types", "3")

if __name__ == "__main__":
    main()
