import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
import pymysql
from pymysql import cursors
import warnings
from dateutil.relativedelta import relativedelta
import os
from io import BytesIO

warnings.filterwarnings('ignore')

# Set page configuration
st.set_page_config(
    page_title="Business Development Performance Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: #374151;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: white;
        border-radius: 10px;
        padding: 1.5rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        border-left: 4px solid #1E3A8A;
    }
    .positive {
        color: #10B981;
        font-weight: 600;
    }
    .negative {
        color: #EF4444;
        font-weight: 600;
    }
    .neutral {
        color: #6B7280;
        font-weight: 600;
    }
    .stDataFrame {
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }
    .stDateInput > div > div > input {
        border-radius: 8px;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 700;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        border-radius: 4px 4px 0px 0px;
        padding: 10px 20px;
    }
</style>
""", unsafe_allow_html=True)

class PerformanceDashboard:
    def __init__(self):
        self.today = datetime.now()
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
        
    def connect_db(self):
        """Connect to MySQL database"""
        try:
            connection = pymysql.connect(
                host='db4free.net',
                user='lamin_d_kinteh',
                password='Lamin@123',
                database='bdp_report',
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            return connection
        except Exception as e:
            st.error(f"Database connection failed: {str(e)}")
            return None
    
    def load_transaction_data(self, start_date, end_date):
        """Load transaction data from MySQL with flexible date range"""
        try:
            conn = self.connect_db()
            if conn:
                with conn.cursor() as cursor:
                    query = """
                    SELECT 
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
                    WHERE created_at BETWEEN %s AND %s
                    """
                    cursor.execute(query, (start_date, end_date))
                    result = cursor.fetchall()
                    df = pd.DataFrame(result)
                    
                    # Convert data types
                    if not df.empty:
                        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
                        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
                        df['before_balance'] = pd.to_numeric(df['before_balance'], errors='coerce')
                        df['after_balance'] = pd.to_numeric(df['after_balance'], errors='coerce')
                        
                        # Clean text columns
                        text_columns = ['user_identifier', 'product_name', 'entity_name', 
                                       'transaction_type', 'ucp_name', 'service_name', 
                                       'status', 'sub_transaction_id']
                        for col in text_columns:
                            if col in df.columns:
                                df[col] = df[col].astype(str).str.strip()
                    
                    conn.close()
                    return df
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Error loading transaction data: {str(e)}")
            return pd.DataFrame()
    
    def load_onboarding_data(self, start_date, end_date):
        """Load onboarding data from MySQL with flexible date range"""
        try:
            conn = self.connect_db()
            if conn:
                with conn.cursor() as cursor:
                    query = """
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
                    cursor.execute(query, (start_date, end_date))
                    result = cursor.fetchall()
                    df = pd.DataFrame(result)
                    
                    # Convert data types
                    if not df.empty:
                        df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')
                        df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
                        
                        # Clean text columns
                        text_columns = ['mobile', 'status', 'kyc_status', 'entity', 'full_name']
                        for col in text_columns:
                            if col in df.columns:
                                df[col] = df[col].astype(str).str.strip()
                    
                    conn.close()
                    return df
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Error loading onboarding data: {str(e)}")
            return pd.DataFrame()
    
    def calculate_executive_snapshot(self, transactions_df, onboarding_df, period_type='custom'):
        """Calculate executive snapshot metrics"""
        metrics = {}
        
        # New registrations by status (from onboarding)
        if not onboarding_df.empty and 'status' in onboarding_df.columns:
            # Filter for customer entities
            customer_onboarding = onboarding_df[onboarding_df['entity'] == 'Customer']
            
            status_counts = customer_onboarding['status'].value_counts()
            metrics['new_customers_active'] = status_counts.get('Active', 0)
            metrics['new_customers_registered'] = status_counts.get('Registered', 0)
            metrics['new_customers_temporary'] = status_counts.get('TemporaryRegister', 0)
            metrics['new_customers_total'] = len(customer_onboarding)
        else:
            metrics['new_customers_active'] = 0
            metrics['new_customers_registered'] = 0
            metrics['new_customers_temporary'] = 0
            metrics['new_customers_total'] = 0
        
        # Active customers (from transactions)
        if not transactions_df.empty:
            # Filter successful customer transactions
            customer_transactions = transactions_df[
                (transactions_df['entity_name'] == 'Customer') &
                (transactions_df['status'] == 'SUCCESS')
            ]
            
            if not customer_transactions.empty:
                # Different thresholds for different period types
                if period_type in ['weekly', 'rolling', 'custom']:
                    threshold = 2
                else:  # monthly
                    threshold = 10
                
                user_counts = customer_transactions['user_identifier'].value_counts()
                active_users = user_counts[user_counts >= threshold].index.tolist()
                metrics['active_customers_all'] = len(active_users)
                
                # Total transactions
                metrics['total_transactions'] = len(customer_transactions)
                # Total amount
                if 'amount' in customer_transactions.columns:
                    metrics['total_amount'] = customer_transactions['amount'].sum()
                else:
                    metrics['total_amount'] = 0
            else:
                metrics['active_customers_all'] = 0
                metrics['total_transactions'] = 0
                metrics['total_amount'] = 0
        else:
            metrics['active_customers_all'] = 0
            metrics['total_transactions'] = 0
            metrics['total_amount'] = 0
        
        # WAU calculation (Weekly Active Users from new customers)
        if metrics['new_customers_total'] > 0:
            # Estimate WAU based on new customers who transacted
            new_customer_ids = onboarding_df['mobile'].unique() if not onboarding_df.empty else []
            if new_customer_ids.any() and not transactions_df.empty:
                wau_customers = transactions_df[
                    (transactions_df['user_identifier'].isin(new_customer_ids)) &
                    (transactions_df['status'] == 'SUCCESS')
                ]['user_identifier'].nunique()
                metrics['wau_total'] = wau_customers
            else:
                metrics['wau_total'] = 0
        else:
            metrics['wau_total'] = 0
        
        # Top and low performing products
        if not transactions_df.empty and 'product_name' in transactions_df.columns:
            # Filter successful customer transactions
            product_transactions = transactions_df[
                (transactions_df['entity_name'] == 'Customer') &
                (transactions_df['status'] == 'SUCCESS')
            ]
            
            if not product_transactions.empty:
                product_counts = product_transactions['product_name'].value_counts()
                
                if not product_counts.empty:
                    metrics['top_product'] = product_counts.index[0]
                    metrics['top_product_count'] = int(product_counts.iloc[0])
                    
                    # Get product amount
                    top_product_transactions = product_transactions[
                        product_transactions['product_name'] == metrics['top_product']
                    ]
                    metrics['top_product_amount'] = top_product_transactions['amount'].sum() if 'amount' in top_product_transactions.columns else 0
                    metrics['top_product_users'] = top_product_transactions['user_identifier'].nunique()
                    
                    active_products = product_counts[product_counts > 0]
                    if not active_products.empty:
                        metrics['low_product'] = active_products.index[-1]
                        metrics['low_product_count'] = int(active_products.iloc[-1])
                        
                        # Get low product amount
                        low_product_transactions = product_transactions[
                            product_transactions['product_name'] == metrics['low_product']
                        ]
                        metrics['low_product_amount'] = low_product_transactions['amount'].sum() if 'amount' in low_product_transactions.columns else 0
                        metrics['low_product_users'] = low_product_transactions['user_identifier'].nunique()
                    else:
                        metrics['low_product'] = 'N/A'
                        metrics['low_product_count'] = 0
                        metrics['low_product_amount'] = 0
                        metrics['low_product_users'] = 0
            else:
                metrics['top_product'] = 'N/A'
                metrics['top_product_count'] = 0
                metrics['top_product_amount'] = 0
                metrics['top_product_users'] = 0
                metrics['low_product'] = 'N/A'
                metrics['low_product_count'] = 0
                metrics['low_product_amount'] = 0
                metrics['low_product_users'] = 0
        
        return metrics
    
    def calculate_customer_acquisition(self, transactions_df, onboarding_df):
        """Calculate customer acquisition metrics"""
        metrics = {}
        
        # Filter for customer entities
        if not onboarding_df.empty:
            customer_onboarding = onboarding_df[onboarding_df['entity'] == 'Customer']
        else:
            customer_onboarding = pd.DataFrame()
        
        # New registrations by status
        if not customer_onboarding.empty and 'status' in customer_onboarding.columns:
            status_counts = customer_onboarding['status'].value_counts()
            metrics['new_registrations_active'] = status_counts.get('Active', 0)
            metrics['new_registrations_registered'] = status_counts.get('Registered', 0)
            metrics['new_registrations_temporary'] = status_counts.get('TemporaryRegister', 0)
            metrics['new_registrations_total'] = len(customer_onboarding)
        else:
            metrics['new_registrations_active'] = 0
            metrics['new_registrations_registered'] = 0
            metrics['new_registrations_temporary'] = 0
            metrics['new_registrations_total'] = 0
        
        # KYC Completed
        if not customer_onboarding.empty and 'kyc_status' in customer_onboarding.columns:
            kyc_completed = customer_onboarding[customer_onboarding['kyc_status'].str.upper() == 'VERIFIED']
            metrics['kyc_completed'] = len(kyc_completed)
        else:
            metrics['kyc_completed'] = 0
        
        # First-Time Transactors (FTT)
        if not customer_onboarding.empty and not transactions_df.empty:
            # Get new customer mobile numbers
            new_customer_mobiles = customer_onboarding['mobile'].dropna().unique()
            
            # Get successful transactions
            successful_transactions = transactions_df[
                (transactions_df['status'] == 'SUCCESS') &
                (transactions_df['entity_name'] == 'Customer')
            ]
            
            if len(new_customer_mobiles) > 0 and not successful_transactions.empty:
                # Match mobile numbers with user identifiers
                transacting_users = successful_transactions['user_identifier'].unique()
                ftt = len(set(new_customer_mobiles) & set(transacting_users))
                metrics['ftt'] = ftt
            else:
                metrics['ftt'] = 0
        else:
            metrics['ftt'] = 0
        
        # FTT Rate
        if metrics['new_registrations_total'] > 0:
            metrics['ftt_rate'] = (metrics['ftt'] / metrics['new_registrations_total']) * 100
        else:
            metrics['ftt_rate'] = 0
        
        # Activation Rate (simplified)
        if not transactions_df.empty:
            total_transacting_customers = transactions_df[
                transactions_df['entity_name'] == 'Customer'
            ]['user_identifier'].nunique()
            metrics['total_transacting_customers'] = total_transacting_customers
        else:
            metrics['total_transacting_customers'] = 0
        
        return metrics
    
    def calculate_product_metrics(self, transactions_df):
        """Calculate product usage metrics"""
        if transactions_df.empty:
            return {}
        
        product_metrics = {}
        
        # Process regular products
        for category, products in self.product_categories.items():
            for product in products:
                if product == 'Internal Wallet Transfer':
                    # P2P special handling - only DR transactions, exclude fees
                    product_trans = transactions_df[
                        (transactions_df['product_name'] == 'Internal Wallet Transfer') &
                        (transactions_df['entity_name'] == 'Customer') &
                        (transactions_df['status'] == 'SUCCESS') &
                        (transactions_df['transaction_type'] == 'DR')
                    ]
                    
                    # Exclude fee transactions
                    if 'ucp_name' in product_trans.columns:
                        product_trans = product_trans[
                            ~product_trans['ucp_name'].astype(str).str.contains('Fee', case=False, na=False)
                        ]
                else:
                    product_trans = transactions_df[
                        (transactions_df['product_name'] == product) &
                        (transactions_df['entity_name'] == 'Customer') &
                        (transactions_df['status'] == 'SUCCESS')
                    ]
                
                if not product_trans.empty:
                    total_transactions = len(product_trans)
                    total_amount = product_trans['amount'].sum() if 'amount' in product_trans.columns else 0
                    total_users = product_trans['user_identifier'].nunique()
                    avg_amount = total_amount / total_transactions if total_transactions > 0 else 0
                    
                    # Active users (with ≥2 transactions)
                    user_counts = product_trans['user_identifier'].value_counts()
                    active_users = len(user_counts[user_counts >= 2])
                    
                    product_metrics[product] = {
                        'category': category,
                        'total_transactions': total_transactions,
                        'total_amount': total_amount,
                        'avg_amount': avg_amount,
                        'total_users': total_users,
                        'active_users': active_users,
                        'avg_transactions_per_user': total_transactions / total_users if total_users > 0 else 0
                    }
        
        # Process Airtime Topup (Service)
        for service in self.services:
            service_trans = transactions_df[
                (transactions_df['service_name'] == 'Airtime Topup') &
                (transactions_df['entity_name'] == 'Customer') &
                (transactions_df['status'] == 'SUCCESS') &
                (transactions_df['transaction_type'] == 'DR')
            ]
            
            if not service_trans.empty:
                total_transactions = len(service_trans)
                total_amount = service_trans['amount'].sum() if 'amount' in service_trans.columns else 0
                total_users = service_trans['user_identifier'].nunique()
                avg_amount = total_amount / total_transactions if total_transactions > 0 else 0
                
                # Active users
                user_counts = service_trans['user_identifier'].value_counts()
                active_users = len(user_counts[user_counts >= 2])
                
                product_metrics[service] = {
                    'category': 'Airtime Topup',
                    'total_transactions': total_transactions,
                    'total_amount': total_amount,
                    'avg_amount': avg_amount,
                    'total_users': total_users,
                    'active_users': active_users,
                    'avg_transactions_per_user': total_transactions / total_users if total_users > 0 else 0
                }
        
        return product_metrics
    
    def create_period_selector(self):
        """Create flexible date range selector"""
        st.markdown('<h3 class="sub-header">📅 Select Analysis Period</h3>', 
                   unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            period_type = st.selectbox(
                "Select Period Type",
                ["Custom Range", "Last 7 Days", "Last 30 Days", "Last 90 Days", 
                 "This Month", "Last Month", "This Quarter", "Last Quarter",
                 "Last 6 Months", "Year to Date", "Last Year"],
                index=0
            )
        
        with col2:
            if period_type == "Custom Range":
                start_date = st.date_input("Start Date", value=self.today - timedelta(days=30))
                end_date = st.date_input("End Date", value=self.today)
            elif period_type == "Last 7 Days":
                start_date = self.today - timedelta(days=7)
                end_date = self.today
                st.write(f"**Period:** Last 7 Days")
            elif period_type == "Last 30 Days":
                start_date = self.today - timedelta(days=30)
                end_date = self.today
                st.write(f"**Period:** Last 30 Days")
            elif period_type == "Last 90 Days":
                start_date = self.today - timedelta(days=90)
                end_date = self.today
                st.write(f"**Period:** Last 90 Days")
            elif period_type == "This Month":
                start_date = datetime(self.today.year, self.today.month, 1)
                end_date = self.today
                st.write(f"**Period:** This Month")
            elif period_type == "Last Month":
                if self.today.month == 1:
                    start_date = datetime(self.today.year - 1, 12, 1)
                else:
                    start_date = datetime(self.today.year, self.today.month - 1, 1)
                end_date = start_date + relativedelta(months=1, days=-1)
                st.write(f"**Period:** Last Month")
            elif period_type == "This Quarter":
                quarter = (self.today.month - 1) // 3 + 1
                start_date = datetime(self.today.year, 3 * quarter - 2, 1)
                end_date = self.today
                st.write(f"**Period:** This Quarter (Q{quarter})")
            elif period_type == "Last Quarter":
                quarter = (self.today.month - 1) // 3
                if quarter == 0:
                    start_date = datetime(self.today.year - 1, 10, 1)
                else:
                    start_date = datetime(self.today.year, 3 * quarter - 2, 1)
                end_date = start_date + relativedelta(months=3, days=-1)
                st.write(f"**Period:** Last Quarter (Q{quarter})")
            elif period_type == "Last 6 Months":
                start_date = self.today - timedelta(days=180)
                end_date = self.today
                st.write(f"**Period:** Last 6 Months")
            elif period_type == "Year to Date":
                start_date = datetime(self.today.year, 1, 1)
                end_date = self.today
                st.write(f"**Period:** Year to Date")
            elif period_type == "Last Year":
                start_date = datetime(self.today.year - 1, 1, 1)
                end_date = datetime(self.today.year - 1, 12, 31)
                st.write(f"**Period:** Last Year")
        
        with col3:
            st.markdown("### Date Range Summary")
            st.markdown(f"""
            <div class="metric-card">
                <p><strong>From:</strong> {start_date.strftime('%B %d, %Y')}</p>
                <p><strong>To:</strong> {end_date.strftime('%B %d, %Y')}</p>
                <p><strong>Days Covered:</strong> {(end_date - start_date).days + 1}</p>
            </div>
            """, unsafe_allow_html=True)
        
        return start_date, end_date, period_type
    
    def display_executive_snapshot(self, metrics):
        """Display executive snapshot metrics"""
        st.markdown("### 📊 Executive Snapshot")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="New Customers",
                value=f"{metrics.get('new_customers_total', 0):,}",
                delta=f"{metrics.get('new_customers_active', 0):,} Active",
                delta_color="normal"
            )
        
        with col2:
            st.metric(
                label="Active Customers",
                value=f"{metrics.get('active_customers_all', 0):,}",
                delta=None
            )
        
        with col3:
            st.metric(
                label="Total Transactions",
                value=f"{metrics.get('total_transactions', 0):,}",
                delta=f"${metrics.get('total_amount', 0):,.0f}",
                delta_color="off"
            )
        
        with col4:
            top_product = metrics.get('top_product', 'N/A')
            top_count = metrics.get('top_product_count', 0)
            st.metric(
                label="Top Product",
                value=top_product[:15] + "..." if len(str(top_product)) > 15 else top_product,
                delta=f"{top_count:,} txn"
            )
        
        # Second row of metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="WAU",
                value=f"{metrics.get('wau_total', 0):,}",
                delta=None
            )
        
        with col2:
            st.metric(
                label="Lowest Product",
                value=metrics.get('low_product', 'N/A')[:15] + "..." if len(str(metrics.get('low_product', 'N/A'))) > 15 else metrics.get('low_product', 'N/A'),
                delta=f"{metrics.get('low_product_count', 0):,} txn"
            )
        
        with col3:
            st.metric(
                label="Top Product Users",
                value=f"{metrics.get('top_product_users', 0):,}",
                delta=f"${metrics.get('top_product_amount', 0):,.0f}",
                delta_color="off"
            )
        
        with col4:
            if metrics.get('low_product') != 'N/A':
                st.metric(
                    label="Low Product Users",
                    value=f"{metrics.get('low_product_users', 0):,}",
                    delta=f"${metrics.get('low_product_amount', 0):,.0f}",
                    delta_color="off"
                )
            else:
                st.metric(
                    label="Low Product Users",
                    value="0",
                    delta="N/A"
                )
        
        # Detailed breakdown
        with st.expander("📋 View Detailed Breakdown", expanded=False):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("**New Customers by Status:**")
                st.write(f"• **Active:** {metrics.get('new_customers_active', 0):,}")
                st.write(f"• **Registered:** {metrics.get('new_customers_registered', 0):,}")
                st.write(f"• **Temporary:** {metrics.get('new_customers_temporary', 0):,}")
                st.write(f"• **Total:** {metrics.get('new_customers_total', 0):,}")
            
            with col2:
                st.markdown("**Transaction Summary:**")
                st.write(f"• **Total Transactions:** {metrics.get('total_transactions', 0):,}")
                st.write(f"• **Total Amount:** ${metrics.get('total_amount', 0):,.2f}")
                st.write(f"• **Active Customers:** {metrics.get('active_customers_all', 0):,}")
                st.write(f"• **WAU:** {metrics.get('wau_total', 0):,}")
            
            with col3:
                st.markdown("**Product Performance:**")
                st.write(f"• **Top Product:** {metrics.get('top_product', 'N/A')}")
                st.write(f"  - Transactions: {metrics.get('top_product_count', 0):,}")
                st.write(f"  - Users: {metrics.get('top_product_users', 0):,}")
                st.write(f"  - Amount: ${metrics.get('top_product_amount', 0):,.2f}")
                
                if metrics.get('low_product') != 'N/A':
                    st.write(f"• **Lowest Product:** {metrics.get('low_product', 'N/A')}")
                    st.write(f"  - Transactions: {metrics.get('low_product_count', 0):,}")
                    st.write(f"  - Users: {metrics.get('low_product_users', 0):,}")
                    st.write(f"  - Amount: ${metrics.get('low_product_amount', 0):,.2f}")
    
    def display_customer_acquisition(self, metrics):
        """Display customer acquisition metrics"""
        st.markdown("### 👥 Customer Acquisition")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="New Registrations",
                value=f"{metrics.get('new_registrations_total', 0):,}",
                delta=f"{metrics.get('new_registrations_active', 0):,} Active"
            )
        
        with col2:
            st.metric(
                label="KYC Completed",
                value=f"{metrics.get('kyc_completed', 0):,}",
                delta=None
            )
        
        with col3:
            st.metric(
                label="First-Time Transactors",
                value=f"{metrics.get('ftt', 0):,}",
                delta=None
            )
        
        with col4:
            ftt_rate = metrics.get('ftt_rate', 0)
            st.metric(
                label="FTT Rate",
                value=f"{ftt_rate:.1f}%",
                delta=None
            )
        
        # Visualization
        with st.expander("📊 View Visualizations", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                # Registration status pie chart
                if metrics['new_registrations_total'] > 0:
                    status_data = {
                        'Status': ['Active', 'Registered', 'Temporary'],
                        'Count': [
                            metrics.get('new_registrations_active', 0),
                            metrics.get('new_registrations_registered', 0),
                            metrics.get('new_registrations_temporary', 0)
                        ]
                    }
                    df = pd.DataFrame(status_data)
                    fig = px.pie(df, values='Count', names='Status', 
                               title='Registration Status Distribution',
                               color_discrete_sequence=px.colors.qualitative.Set2,
                               hole=0.3)
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # FTT vs Non-FTT bar chart
                if metrics['new_registrations_total'] > 0:
                    ftt_data = {
                        'Category': ['FTT', 'Non-FTT'],
                        'Count': [
                            metrics.get('ftt', 0),
                            metrics['new_registrations_total'] - metrics.get('ftt', 0)
                        ]
                    }
                    df = pd.DataFrame(ftt_data)
                    fig = px.bar(df, x='Category', y='Count', 
                               title='First-Time Transactors vs Non-Transactors',
                               color='Category',
                               color_discrete_sequence=['#1E3A8A', '#6B7280'],
                               text='Count')
                    fig.update_traces(texttemplate='%{text:,}', textposition='outside')
                    fig.update_layout(yaxis_title='Count')
                    st.plotly_chart(fig, use_container_width=True)
            
            # KYC Status
            if 'kyc_completed' in metrics:
                kyc_data = {
                    'Status': ['KYC Completed', 'KYC Pending'],
                    'Count': [
                        metrics.get('kyc_completed', 0),
                        max(0, metrics.get('new_registrations_total', 0) - metrics.get('kyc_completed', 0))
                    ]
                }
                df = pd.DataFrame(kyc_data)
                fig = px.bar(df, x='Status', y='Count',
                           title='KYC Status Distribution',
                           color='Status',
                           color_discrete_sequence=['#10B981', '#EF4444'],
                           text='Count')
                fig.update_traces(texttemplate='%{text:,}', textposition='outside')
                st.plotly_chart(fig, use_container_width=True)
    
    def display_product_analysis(self, product_metrics):
        """Display product analysis"""
        st.markdown("### 📈 Product Performance Analysis")
        
        if not product_metrics:
            st.info("No product data available for the selected period.")
            return
        
        # Convert to DataFrame
        product_data = []
        for product, metrics in product_metrics.items():
            product_data.append({
                'Product': product,
                'Category': metrics.get('category', ''),
                'Transactions': metrics.get('total_transactions', 0),
                'Amount ($)': metrics.get('total_amount', 0),
                'Avg Amount ($)': metrics.get('avg_amount', 0),
                'Total Users': metrics.get('total_users', 0),
                'Active Users': metrics.get('active_users', 0),
                'Avg Txns/User': metrics.get('avg_transactions_per_user', 0)
            })
        
        df = pd.DataFrame(product_data)
        
        # Display in tabs
        tab1, tab2, tab3, tab4 = st.tabs(["📋 Metrics Table", "📊 Visualizations", "📈 Trends", "📥 Export"])
        
        with tab1:
            # Sort by transactions
            df_sorted = df.sort_values('Transactions', ascending=False)
            
            st.dataframe(
                df_sorted,
                column_config={
                    "Product": st.column_config.TextColumn("Product", width="medium"),
                    "Category": st.column_config.TextColumn("Category", width="medium"),
                    "Transactions": st.column_config.NumberColumn("Transactions", format="%,d"),
                    "Amount ($)": st.column_config.NumberColumn("Amount", format="$%,.2f"),
                    "Avg Amount ($)": st.column_config.NumberColumn("Avg Amount", format="$%,.2f"),
                    "Total Users": st.column_config.NumberColumn("Users", format="%,d"),
                    "Active Users": st.column_config.NumberColumn("Active Users", format="%,d"),
                    "Avg Txns/User": st.column_config.NumberColumn("Avg Txns/User", format="%.2f")
                },
                hide_index=True,
                use_container_width=True
            )
            
            # Top performers summary
            st.markdown("#### 🏆 Top Performers")
            top_3 = df_sorted.head(3)
            cols = st.columns(3)
            for idx, (col, (_, row)) in enumerate(zip(cols, top_3.iterrows())):
                with col:
                    st.markdown(f"""
                    <div class="metric-card">
                        <h4>{row['Product'][:20]}{'...' if len(row['Product']) > 20 else ''}</h4>
                        <p><strong>Transactions:</strong> {row['Transactions']:,}</p>
                        <p><strong>Amount:</strong> ${row['Amount ($)']:,.2f}</p>
                        <p><strong>Users:</strong> {row['Total Users']:,}</p>
                    </div>
                    """, unsafe_allow_html=True)
        
        with tab2:
            col1, col2 = st.columns(2)
            
            with col1:
                # Top 10 products by transactions
                top_10 = df_sorted.head(10)
                fig = px.bar(top_10, 
                           x='Product', y='Transactions',
                           title='Top 10 Products by Transactions',
                           color='Category',
                           color_discrete_sequence=px.colors.qualitative.Set3,
                           text='Transactions')
                fig.update_traces(texttemplate='%{text:,}', textposition='outside')
                fig.update_layout(xaxis_tickangle=-45, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Top 10 products by users
                fig = px.bar(top_10, 
                           x='Product', y='Total Users',
                           title='Top 10 Products by Users',
                           color='Category',
                           color_discrete_sequence=px.colors.qualitative.Pastel,
                           text='Total Users')
                fig.update_traces(texttemplate='%{text:,}', textposition='outside')
                fig.update_layout(xaxis_tickangle=-45, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            
            # Amount by category
            category_amount = df.groupby('Category')['Amount ($)'].sum().reset_index()
            category_amount = category_amount.sort_values('Amount ($)', ascending=False)
            
            fig = px.bar(category_amount, 
                       x='Category', y='Amount ($)',
                       title='Transaction Amount by Category',
                       color='Category',
                       color_discrete_sequence=px.colors.qualitative.Set2,
                       text='Amount ($)')
            fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Active vs Total Users
            fig = px.scatter(df, 
                           x='Total Users', y='Active Users',
                           size='Transactions', color='Category',
                           hover_name='Product',
                           title='Active Users vs Total Users',
                           labels={'Total Users': 'Total Users', 'Active Users': 'Active Users (≥2 txns)'})
            st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            st.info("Trend analysis requires historical data comparison. This feature is available in the advanced version.")
            
            # Placeholder for trend analysis
            col1, col2 = st.columns(2)
            with col1:
                # Product growth trend (simplified)
                fig = px.line(df_sorted.head(5), 
                            x='Product', y='Transactions',
                            title='Top 5 Products - Transaction Volume',
                            markers=True)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # User growth trend (simplified)
                fig = px.line(df_sorted.head(5), 
                            x='Product', y='Total Users',
                            title='Top 5 Products - User Adoption',
                            markers=True)
                st.plotly_chart(fig, use_container_width=True)
        
        with tab4:
            st.markdown("#### 📥 Export Product Data")
            
            # Format options
            export_format = st.radio("Select format:", ["CSV", "Excel", "JSON"], horizontal=True)
            
            if export_format == "CSV":
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name="product_performance.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            elif export_format == "Excel":
                excel_buffer = BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Product Performance')
                excel_data = excel_buffer.getvalue()
                st.download_button(
                    label="📥 Download Excel",
                    data=excel_data,
                    file_name="product_performance.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            else:  # JSON
                json_data = df.to_json(orient='records', indent=2)
                st.download_button(
                    label="📥 Download JSON",
                    data=json_data,
                    file_name="product_performance.json",
                    mime="application/json",
                    use_container_width=True
                )
            
            # Preview
            st.markdown("#### 👀 Data Preview")
            st.dataframe(df.head(10), use_container_width=True)
    
    def display_transaction_analysis(self, transactions_df):
        """Display transaction analysis"""
        st.markdown("### 💰 Transaction Analysis")
        
        if transactions_df.empty:
            st.info("No transaction data available for the selected period.")
            return
        
        # Summary metrics
        total_transactions = len(transactions_df)
        successful_transactions = len(transactions_df[transactions_df['status'] == 'SUCCESS'])
        failed_transactions = total_transactions - successful_transactions
        success_rate = (successful_transactions / total_transactions * 100) if total_transactions > 0 else 0
        
        # Amount metrics
        if 'amount' in transactions_df.columns:
            total_amount = transactions_df['amount'].sum()
            avg_amount = transactions_df['amount'].mean() if successful_transactions > 0 else 0
            max_amount = transactions_df['amount'].max()
            min_amount = transactions_df['amount'].min()
        else:
            total_amount = avg_amount = max_amount = min_amount = 0
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Transactions", f"{total_transactions:,}")
        
        with col2:
            st.metric("Successful", f"{successful_transactions:,}")
        
        with col3:
            st.metric("Success Rate", f"{success_rate:.1f}%")
        
        with col4:
            st.metric("Total Amount", f"${total_amount:,.2f}")
        
        # Second row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Failed", f"{failed_transactions:,}")
        
        with col2:
            st.metric("Avg Amount", f"${avg_amount:,.2f}")
        
        with col3:
            st.metric("Max Amount", f"${max_amount:,.2f}")
        
        with col4:
            st.metric("Min Amount", f"${min_amount:,.2f}")
        
        # Detailed analysis in tabs
        tab1, tab2, tab3, tab4 = st.tabs(["📈 Trends", "🔍 Details", "📊 Status", "🧮 Amount Analysis"])
        
        with tab1:
            # Daily transaction trend
            if 'created_at' in transactions_df.columns:
                daily_transactions = transactions_df.copy()
                daily_transactions['date'] = pd.to_datetime(daily_transactions['created_at']).dt.date
                daily_stats = daily_transactions.groupby('date').agg(
                    count=('user_identifier', 'size'),
                    amount=('amount', 'sum'),
                    success_rate=('status', lambda x: (x == 'SUCCESS').mean() * 100)
                ).reset_index()
                
                col1, col2 = st.columns(2)
                
                with col1:
                    fig = px.line(daily_stats, x='date', y='count',
                                title='Daily Transaction Volume',
                                markers=True)
                    fig.update_layout(yaxis_title='Transaction Count')
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    fig = px.line(daily_stats, x='date', y='amount',
                                title='Daily Transaction Amount',
                                markers=True)
                    fig.update_layout(yaxis_title='Amount ($)')
                    st.plotly_chart(fig, use_container_width=True)
                
                # Success rate trend
                fig = px.line(daily_stats, x='date', y='success_rate',
                            title='Daily Success Rate',
                            markers=True)
                fig.update_layout(yaxis_title='Success Rate (%)')
                st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            # Transaction details table
            display_cols = ['created_at', 'user_identifier', 'product_name', 
                          'amount', 'status', 'transaction_type', 'service_name']
            available_cols = [col for col in display_cols if col in transactions_df.columns]
            
            st.dataframe(
                transactions_df[available_cols].head(200),
                column_config={
                    "created_at": st.column_config.DatetimeColumn("Date", format="YYYY-MM-DD HH:mm"),
                    "user_identifier": "User ID",
                    "product_name": "Product",
                    "amount": st.column_config.NumberColumn("Amount", format="$%,.2f"),
                    "status": "Status",
                    "transaction_type": "Type",
                    "service_name": "Service"
                },
                hide_index=True,
                use_container_width=True
            )
            
            # Show record count
            st.caption(f"Showing 200 of {len(transactions_df):,} records")
        
        with tab3:
            # Status distribution
            status_counts = transactions_df['status'].value_counts().reset_index()
            status_counts.columns = ['Status', 'Count']
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.pie(status_counts, values='Count', names='Status',
                           title='Transaction Status Distribution',
                           hole=0.3)
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.bar(status_counts, x='Status', y='Count',
                           title='Transaction Status Count',
                           color='Status',
                           text='Count')
                fig.update_traces(texttemplate='%{text:,}', textposition='outside')
                st.plotly_chart(fig, use_container_width=True)
            
            # Status by product
            if 'product_name' in transactions_df.columns:
                status_by_product = pd.crosstab(
                    transactions_df['product_name'], 
                    transactions_df['status']
                ).reset_index()
                
                # Show top 10 products
                top_products = transactions_df['product_name'].value_counts().head(10).index
                status_by_product_top = status_by_product[status_by_product['product_name'].isin(top_products)]
                
                fig = px.bar(status_by_product_top.melt(id_vars='product_name'), 
                           x='product_name', y='value', color='status',
                           title='Transaction Status by Product (Top 10)',
                           barmode='stack')
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
        
        with tab4:
            # Amount distribution
            if 'amount' in transactions_df.columns and successful_transactions > 0:
                successful_df = transactions_df[transactions_df['status'] == 'SUCCESS']
                
                col1, col2 = st.columns(2)
                
                with col1:
                    fig = px.histogram(successful_df, x='amount',
                                     title='Transaction Amount Distribution',
                                     nbins=50)
                    fig.update_layout(xaxis_title='Amount ($)', yaxis_title='Count')
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Box plot by product (top 10)
                    top_products = successful_df['product_name'].value_counts().head(10).index
                    top_products_df = successful_df[successful_df['product_name'].isin(top_products)]
                    
                    fig = px.box(top_products_df, x='product_name', y='amount',
                               title='Amount Distribution by Product (Top 10)')
                    fig.update_layout(xaxis_title='Product', yaxis_title='Amount ($)',
                                    xaxis_tickangle=-45)
                    st.plotly_chart(fig, use_container_width=True)
                
                # Amount statistics table
                amount_stats = successful_df['amount'].describe().reset_index()
                amount_stats.columns = ['Statistic', 'Value']
                amount_stats['Value'] = amount_stats['Value'].apply(lambda x: f"${x:,.2f}")
                st.dataframe(amount_stats, hide_index=True, use_container_width=True)
    
    def run_dashboard(self):
        """Main dashboard function"""
        # Header
        st.markdown('<h1 class="main-header">Business Development Performance Dashboard</h1>', 
                   unsafe_allow_html=True)
        st.markdown("*Real-time analytics for customer acquisition and product performance*")
        st.markdown("---")
        
        # Sidebar for filters and options
        with st.sidebar:
            st.markdown("### 🔧 Dashboard Controls")
            
            # Refresh button
            if st.button("🔄 Refresh Data", use_container_width=True, type="primary"):
                st.rerun()
            
            st.markdown("---")
            
            # Data source info
            st.markdown("#### 📊 Data Source")
            st.info(f"Connected to MySQL database\n\n**Host:** db4free.net\n**Database:** bdp_report\n**Tables:** Transaction, Onboarding")
            
            st.markdown("---")
            
            # Additional filters
            st.markdown("#### 🎯 Advanced Filters")
            
            # Entity filter
            entity_filter = st.multiselect(
                "Filter by Entity",
                ["Customer", "Agent", "Merchant"],
                default=["Customer"]
            )
            
            # Status filter
            status_filter = st.multiselect(
                "Filter by Status",
                ["SUCCESS", "FAILED", "PENDING"],
                default=["SUCCESS"]
            )
            
            # Product category filter
            categories = list(self.product_categories.keys())
            selected_categories = st.multiselect(
                "Filter by Product Category",
                categories,
                default=categories
            )
            
            st.markdown("---")
            
            # Display options
            st.markdown("#### 👁️ Display Options")
            show_raw_data = st.checkbox("Show Raw Data Preview", value=False)
            show_metrics_details = st.checkbox("Show Detailed Metrics", value=True)
            
            st.markdown("---")
            
            # Export options
            st.markdown("#### 📥 Export Options")
            if st.button("📊 Export Full Report", use_container_width=True):
                st.info("Export functionality requires additional configuration")
        
        # Date range selection
        start_date, end_date, period_type = self.create_period_selector()
        
        st.markdown("---")
        
        # Load data with progress indicator
        with st.spinner(f"Loading data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}..."):
            transactions_df = self.load_transaction_data(start_date, end_date)
            onboarding_df = self.load_onboarding_data(start_date, end_date)
        
        # Data summary
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Transactions Loaded", f"{len(transactions_df):,}")
        with col2:
            st.metric("Onboarding Records", f"{len(onboarding_df):,}")
        with col3:
            days_covered = (end_date - start_date).days + 1
            st.metric("Period Coverage", f"{days_covered} days")
        
        # Check if data is loaded
        if transactions_df.empty and onboarding_df.empty:
            st.warning("⚠️ No data found for the selected period. Please adjust your date range or check database connection.")
            st.info("💡 Try selecting a different date range or check if the database contains data for this period.")
            return
        
        # Display metrics in tabs
        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 Executive Overview", 
            "👥 Customer Acquisition", 
            "📈 Product Analysis",
            "💰 Transaction Details"
        ])
        
        with tab1:
            # Calculate executive metrics
            exec_metrics = self.calculate_executive_snapshot(
                transactions_df, onboarding_df, period_type
            )
            self.display_executive_snapshot(exec_metrics)
            
            # Quick insights
            with st.expander("💡 Quick Insights", expanded=False):
                if exec_metrics.get('new_customers_total', 0) > 0:
                    st.success(f"🎯 **Customer Growth:** {exec_metrics['new_customers_total']:,} new customers acquired")
                
                if exec_metrics.get('active_customers_all', 0) > 0:
                    activity_rate = (exec_metrics['active_customers_all'] / max(exec_metrics['new_customers_total'], 1)) * 100
                    st.info(f"📈 **Activity Rate:** {activity_rate:.1f}% of new customers are active")
                
                if exec_metrics.get('top_product') != 'N/A':
                    st.info(f"🏆 **Top Performer:** {exec_metrics['top_product']} leads with {exec_metrics['top_product_count']:,} transactions")
        
        with tab2:
            # Calculate acquisition metrics
            acquisition_metrics = self.calculate_customer_acquisition(
                transactions_df, onboarding_df
            )
            self.display_customer_acquisition(acquisition_metrics)
            
            # Acquisition insights
            with st.expander("🎯 Acquisition Insights", expanded=False):
                if acquisition_metrics['new_registrations_total'] > 0:
                    conversion_rate = (acquisition_metrics['ftt'] / acquisition_metrics['new_registrations_total']) * 100
                    st.metric("Registration to FTT Conversion", f"{conversion_rate:.1f}%")
                
                if acquisition_metrics['kyc_completed'] > 0:
                    kyc_rate = (acquisition_metrics['kyc_completed'] / acquisition_metrics['new_registrations_total']) * 100
                    st.metric("KYC Completion Rate", f"{kyc_rate:.1f}%")
        
        with tab3:
            # Calculate product metrics
            product_metrics = self.calculate_product_metrics(transactions_df)
            self.display_product_analysis(product_metrics)
            
            # Product insights
            if product_metrics:
                with st.expander("📊 Product Insights", expanded=False):
                    # Find most adopted product
                    if product_metrics:
                        most_adopted = max(product_metrics.items(), key=lambda x: x[1]['total_users'])
                        st.success(f"👥 **Most Adopted:** {most_adopted[0]} with {most_adopted[1]['total_users']:,} users")
                        
                        # Find highest value product
                        highest_value = max(product_metrics.items(), key=lambda x: x[1]['total_amount'])
                        st.info(f"💰 **Highest Value:** {highest_value[0]} with ${highest_value[1]['total_amount']:,.2f}")
        
        with tab4:
            self.display_transaction_analysis(transactions_df)
            
            # Transaction insights
            with st.expander("💎 Transaction Insights", expanded=False):
                if not transactions_df.empty:
                    # Success rate insight
                    success_rate = (len(transactions_df[transactions_df['status'] == 'SUCCESS']) / len(transactions_df)) * 100
                    if success_rate > 90:
                        st.success(f"✅ **Excellent Success Rate:** {success_rate:.1f}%")
                    elif success_rate > 75:
                        st.info(f"⚠️ **Good Success Rate:** {success_rate:.1f}%")
                    else:
                        st.warning(f"🔴 **Low Success Rate:** {success_rate:.1f}% - Needs attention")
        
        # Raw data preview
        if show_raw_data:
            st.markdown("---")
            st.markdown("### 📁 Raw Data Preview")
            
            data_tab1, data_tab2 = st.tabs(["Transaction Data", "Onboarding Data"])
            
            with data_tab1:
                if not transactions_df.empty:
                    st.dataframe(transactions_df.head(100), use_container_width=True)
                    st.caption(f"Showing 100 of {len(transactions_df):,} transaction records")
                else:
                    st.info("No transaction data available")
            
            with data_tab2:
                if not onboarding_df.empty:
                    st.dataframe(onboarding_df.head(100), use_container_width=True)
                    st.caption(f"Showing 100 of {len(onboarding_df):,} onboarding records")
                else:
                    st.info("No onboarding data available")
        
        # Footer
        st.markdown("---")
        st.markdown(
            """
            <div style='text-align: center; color: #6B7280; font-size: 0.9rem; padding: 2rem;'>
                <p>📊 <strong>Business Development Performance Dashboard</strong> | Version 2.0</p>
                <p>📍 Real-time analytics powered by MySQL | Last updated: {}</p>
                <p>📧 For support: analytics@company.com | 📞 +220 123 4567</p>
            </div>
            """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            unsafe_allow_html=True
        )

def main():
    """Main function to run the dashboard"""
    try:
        # Initialize dashboard
        dashboard = PerformanceDashboard()
        
        # Run dashboard
        dashboard.run_dashboard()
        
    except Exception as e:
        st.error(f"🚨 An error occurred: {str(e)}")
        st.info("""
        **Troubleshooting steps:**
        1. Check your database connection credentials
        2. Verify that the tables 'Transaction' and 'Onboarding' exist in the database
        3. Ensure you have internet connectivity
        4. Try refreshing the page
        
        If the issue persists, contact technical support.
        """)

if __name__ == "__main__":
    main()
