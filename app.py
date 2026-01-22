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
                        id, user_identifier, transaction_id, sub_transaction_id,
                        entity_name, full_name, created_by, status, internal_status,
                        service_name, product_name, transaction_type, amount,
                        before_balance, after_balance, ucp_name, wallet_name,
                        pouch_name, reference, error_code, error_message,
                        vendor_transaction_id, vendor_response_code, vendor_message,
                        slug, remarks, created_at, business_hierarchy,
                        parent_user_identifier, parent_full_name
                    FROM Transaction
                    WHERE created_at BETWEEN %s AND %s
                    """
                    cursor.execute(query, (start_date, end_date))
                    result = cursor.fetchall()
                    df = pd.DataFrame(result)
                    
                    # Convert data types
                    if not df.empty:
                        df['created_at'] = pd.to_datetime(df['created_at'])
                        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
                        
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
                        account_id, full_name, mobile, email, region, district,
                        town_village, business_name, kyc_status, registration_date,
                        updated_at, proof_of_id, identification_number,
                        customer_referrer_code, customer_referrer_mobile,
                        referrer_entity, entity, bank, bank_account_name,
                        bank_account_number, status
                    FROM Onboarding
                    WHERE registration_date BETWEEN %s AND %s
                    """
                    cursor.execute(query, (start_date, end_date))
                    result = cursor.fetchall()
                    df = pd.DataFrame(result)
                    
                    # Convert data types
                    if not df.empty:
                        df['registration_date'] = pd.to_datetime(df['registration_date'])
                        df['updated_at'] = pd.to_datetime(df['updated_at'])
                        
                    conn.close()
                    return df
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Error loading onboarding data: {str(e)}")
            return pd.DataFrame()
    
    def calculate_executive_snapshot(self, transactions_df, onboarding_df, period_type='custom'):
        """Calculate executive snapshot metrics"""
        metrics = {}
        
        # New registrations by status
        if not onboarding_df.empty and 'status' in onboarding_df.columns:
            status_counts = onboarding_df['status'].value_counts()
            metrics['new_customers_active'] = status_counts.get('Active', 0)
            metrics['new_customers_registered'] = status_counts.get('Registered', 0)
            metrics['new_customers_temporary'] = status_counts.get('TemporaryRegister', 0)
            metrics['new_customers_total'] = len(onboarding_df)
        else:
            metrics['new_customers_active'] = 0
            metrics['new_customers_registered'] = 0
            metrics['new_customers_temporary'] = 0
            metrics['new_customers_total'] = 0
        
        # Active customers
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
            else:
                metrics['active_customers_all'] = 0
        else:
            metrics['active_customers_all'] = 0
        
        # WAU calculation
        wau_by_status = {'Active': 0, 'Registered': 0, 'TemporaryRegister': 0}
        
        for status in ['Active', 'Registered', 'TemporaryRegister']:
            if status in metrics:
                status_count = metrics[f'new_customers_{status.lower()}']
                if status_count > 0:
                    # Simplified WAU calculation
                    wau_by_status[status] = int(status_count * 0.7)  # Assume 70% activity rate
        
        metrics['wau_total'] = sum(wau_by_status.values())
        
        # Top and low performing products
        if not transactions_df.empty and 'product_name' in transactions_df.columns:
            product_transactions = transactions_df[
                (transactions_df['entity_name'] == 'Customer') &
                (transactions_df['status'] == 'SUCCESS')
            ]
            
            if not product_transactions.empty:
                product_counts = product_transactions['product_name'].value_counts()
                
                if not product_counts.empty:
                    metrics['top_product'] = product_counts.index[0]
                    metrics['top_product_count'] = int(product_counts.iloc[0])
                    
                    active_products = product_counts[product_counts > 0]
                    if not active_products.empty:
                        metrics['low_product'] = active_products.index[-1]
                        metrics['low_product_count'] = int(active_products.iloc[-1])
                    else:
                        metrics['low_product'] = 'N/A'
                        metrics['low_product_count'] = 0
        
        return metrics
    
    def calculate_customer_acquisition(self, transactions_df, onboarding_df):
        """Calculate customer acquisition metrics"""
        metrics = {}
        
        # New registrations by status
        if not onboarding_df.empty and 'status' in onboarding_df.columns:
            status_counts = onboarding_df['status'].value_counts()
            metrics['new_registrations_active'] = status_counts.get('Active', 0)
            metrics['new_registrations_registered'] = status_counts.get('Registered', 0)
            metrics['new_registrations_temporary'] = status_counts.get('TemporaryRegister', 0)
            metrics['new_registrations_total'] = len(onboarding_df)
        else:
            metrics['new_registrations_active'] = 0
            metrics['new_registrations_registered'] = 0
            metrics['new_registrations_temporary'] = 0
            metrics['new_registrations_total'] = 0
        
        # KYC Completed
        if not onboarding_df.empty and 'kyc_status' in onboarding_df.columns:
            kyc_completed = onboarding_df[onboarding_df['kyc_status'].str.upper() == 'VERIFIED']
            metrics['kyc_completed'] = len(kyc_completed)
        else:
            metrics['kyc_completed'] = 0
        
        # First-Time Transactors
        if not onboarding_df.empty and not transactions_df.empty:
            new_customers = onboarding_df['mobile'].unique()
            transacting_customers = transactions_df[
                (transactions_df['entity_name'] == 'Customer') &
                (transactions_df['status'] == 'SUCCESS')
            ]['user_identifier'].unique()
            
            ftt = len(set(new_customers) & set(transacting_customers))
            metrics['ftt'] = ftt
        else:
            metrics['ftt'] = 0
        
        # FTT Rate
        if metrics['new_registrations_total'] > 0:
            metrics['ftt_rate'] = (metrics['ftt'] / metrics['new_registrations_total']) * 100
        else:
            metrics['ftt_rate'] = 0
        
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
                    # P2P special handling
                    product_trans = transactions_df[
                        (transactions_df['product_name'] == 'Internal Wallet Transfer') &
                        (transactions_df['entity_name'] == 'Customer') &
                        (transactions_df['status'] == 'SUCCESS') &
                        (transactions_df['transaction_type'] == 'DR')
                    ]
                    
                    if 'ucp_name' in product_trans.columns:
                        product_trans = product_trans[
                            ~product_trans['ucp_name'].str.contains('Fee', case=False, na=False)
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
                    
                    product_metrics[product] = {
                        'category': category,
                        'total_transactions': total_transactions,
                        'total_amount': total_amount,
                        'avg_amount': avg_amount,
                        'total_users': total_users,
                        'active_users': total_users  # Simplified for demo
                    }
        
        # Process Airtime Topup
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
                
                product_metrics[service] = {
                    'category': 'Airtime Topup',
                    'total_transactions': total_transactions,
                    'total_amount': total_amount,
                    'avg_amount': avg_amount,
                    'total_users': total_users,
                    'active_users': total_users
                }
        
        return product_metrics
    
    def create_period_selector(self):
        """Create flexible date range selector"""
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            period_type = st.selectbox(
                "Select Period Type",
                ["Custom Range", "Last 7 Days", "Last 30 Days", "Last 90 Days", 
                 "This Month", "Last Month", "This Quarter", "Last Quarter"],
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
        
        with col3:
            st.write("### Date Range")
            st.write(f"**From:** {start_date.strftime('%Y-%m-%d')}")
            st.write(f"**To:** {end_date.strftime('%Y-%m-%d')}")
            days_diff = (end_date - start_date).days + 1
            st.write(f"**Days Covered:** {days_diff}")
        
        return start_date, end_date, period_type
    
    def display_executive_snapshot(self, metrics):
        """Display executive snapshot metrics"""
        st.markdown("### 📊 Executive Snapshot")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="New Customers (Total)",
                value=f"{metrics.get('new_customers_total', 0):,}",
                delta=f"{metrics.get('new_customers_active', 0):,} Active"
            )
        
        with col2:
            st.metric(
                label="Active Customers",
                value=f"{metrics.get('active_customers_all', 0):,}",
                delta=None
            )
        
        with col3:
            st.metric(
                label="WAU (New Customers)",
                value=f"{metrics.get('wau_total', 0):,}",
                delta=None
            )
        
        with col4:
            top_product = metrics.get('top_product', 'N/A')
            top_count = metrics.get('top_product_count', 0)
            st.metric(
                label="Top Product",
                value=top_product[:20] + "..." if len(str(top_product)) > 20 else top_product,
                delta=f"{top_count:,} txn"
            )
        
        # Detailed breakdown
        with st.expander("View Detailed Breakdown"):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("**New Customers by Status:**")
                st.write(f"• Active: {metrics.get('new_customers_active', 0):,}")
                st.write(f"• Registered: {metrics.get('new_customers_registered', 0):,}")
                st.write(f"• Temporary: {metrics.get('new_customers_temporary', 0):,}")
            
            with col2:
                st.markdown("**Product Performance:**")
                st.write(f"• Top: {metrics.get('top_product', 'N/A')} ({metrics.get('top_product_count', 0):,})")
                st.write(f"• Low: {metrics.get('low_product', 'N/A')} ({metrics.get('low_product_count', 0):,})")
            
            with col3:
                st.markdown("**Activity Metrics:**")
                st.write(f"• Total Transactions: {metrics.get('total_transactions', 0):,}")
    
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
        
        # Registration breakdown
        with st.expander("View Registration Details"):
            col1, col2 = st.columns(2)
            
            with col1:
                data = {
                    'Status': ['Active', 'Registered', 'Temporary'],
                    'Count': [
                        metrics.get('new_registrations_active', 0),
                        metrics.get('new_registrations_registered', 0),
                        metrics.get('new_registrations_temporary', 0)
                    ]
                }
                df = pd.DataFrame(data)
                fig = px.pie(df, values='Count', names='Status', 
                           title='Registration Status Distribution',
                           color_discrete_sequence=px.colors.qualitative.Set2)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                if metrics['new_registrations_total'] > 0:
                    data = {
                        'Category': ['FTT', 'Non-FTT'],
                        'Count': [
                            metrics.get('ftt', 0),
                            metrics['new_registrations_total'] - metrics.get('ftt', 0)
                        ]
                    }
                    df = pd.DataFrame(data)
                    fig = px.bar(df, x='Category', y='Count', 
                               title='First-Time Transactors vs Non-Transactors',
                               color='Category',
                               color_discrete_sequence=['#1E3A8A', '#6B7280'])
                    st.plotly_chart(fig, use_container_width=True)
    
    def display_product_analysis(self, product_metrics):
        """Display product analysis"""
        st.markdown("### 📈 Product Performance")
        
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
                'Amount': metrics.get('total_amount', 0),
                'Avg Amount': metrics.get('avg_amount', 0),
                'Users': metrics.get('total_users', 0),
                'Active Users': metrics.get('active_users', 0)
            })
        
        df = pd.DataFrame(product_data)
        
        # Display product metrics
        tab1, tab2, tab3 = st.tabs(["📋 Product Metrics", "📊 Visualizations", "📥 Export Data"])
        
        with tab1:
            # Sort by transactions
            df_sorted = df.sort_values('Transactions', ascending=False)
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.dataframe(
                    df_sorted,
                    column_config={
                        "Product": st.column_config.TextColumn("Product", width="medium"),
                        "Category": st.column_config.TextColumn("Category", width="medium"),
                        "Transactions": st.column_config.NumberColumn("Transactions", format="%,d"),
                        "Amount": st.column_config.NumberColumn("Amount", format="%,.2f"),
                        "Avg Amount": st.column_config.NumberColumn("Avg Amount", format="%,.2f"),
                        "Users": st.column_config.NumberColumn("Users", format="%,d"),
                        "Active Users": st.column_config.NumberColumn("Active Users", format="%,d")
                    },
                    hide_index=True,
                    use_container_width=True
                )
            
            with col2:
                st.markdown("**Top Performers:**")
                top_3 = df_sorted.head(3)
                for idx, row in top_3.iterrows():
                    st.metric(
                        label=row['Product'][:15] + "..." if len(row['Product']) > 15 else row['Product'],
                        value=f"{row['Transactions']:,}",
                        delta=f"{row['Users']:,} users"
                    )
        
        with tab2:
            col1, col2 = st.columns(2)
            
            with col1:
                # Transactions by product
                fig = px.bar(df_sorted.head(10), 
                           x='Product', y='Transactions',
                           title='Top 10 Products by Transactions',
                           color='Category',
                           color_discrete_sequence=px.colors.qualitative.Set3)
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Users by product
                fig = px.bar(df_sorted.head(10), 
                           x='Product', y='Users',
                           title='Top 10 Products by Users',
                           color='Category',
                           color_discrete_sequence=px.colors.qualitative.Pastel)
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            
            # Amount by category
            category_amount = df.groupby('Category')['Amount'].sum().reset_index()
            fig = px.pie(category_amount, values='Amount', names='Category',
                       title='Transaction Amount by Category',
                       hole=0.4)
            st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            st.markdown("**Export Product Data**")
            
            # Convert to different formats
            csv = df.to_csv(index=False)
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Product Performance')
            excel_data = excel_buffer.getvalue()
            
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name="product_performance.csv",
                    mime="text/csv"
                )
            with col2:
                st.download_button(
                    label="📥 Download as Excel",
                    data=excel_data,
                    file_name="product_performance.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
    
    def display_transaction_analysis(self, transactions_df):
        """Display transaction analysis"""
        st.markdown("### 💰 Transaction Analysis")
        
        if transactions_df.empty:
            st.info("No transaction data available for the selected period.")
            return
        
        # Summary metrics
        total_transactions = len(transactions_df)
        successful_transactions = len(transactions_df[transactions_df['status'] == 'SUCCESS'])
        success_rate = (successful_transactions / total_transactions * 100) if total_transactions > 0 else 0
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Transactions", f"{total_transactions:,}")
        
        with col2:
            st.metric("Successful", f"{successful_transactions:,}")
        
        with col3:
            st.metric("Success Rate", f"{success_rate:.1f}%")
        
        with col4:
            total_amount = transactions_df['amount'].sum() if 'amount' in transactions_df.columns else 0
            st.metric("Total Amount", f"${total_amount:,.2f}")
        
        # Detailed analysis
        tab1, tab2, tab3 = st.tabs(["📈 Trends", "🔍 Details", "📊 Status Analysis"])
        
        with tab1:
            # Daily transaction trend
            if 'created_at' in transactions_df.columns:
                daily_transactions = transactions_df.copy()
                daily_transactions['date'] = pd.to_datetime(daily_transactions['created_at']).dt.date
                daily_counts = daily_transactions.groupby('date').size().reset_index(name='count')
                
                fig = px.line(daily_counts, x='date', y='count',
                            title='Daily Transaction Trend',
                            markers=True)
                st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            # Transaction details
            st.dataframe(
                transactions_df[['created_at', 'user_identifier', 'product_name', 
                               'amount', 'status', 'transaction_type']].head(100),
                column_config={
                    "created_at": "Date",
                    "user_identifier": "User ID",
                    "product_name": "Product",
                    "amount": st.column_config.NumberColumn("Amount", format="%,.2f"),
                    "status": "Status",
                    "transaction_type": "Type"
                },
                hide_index=True,
                use_container_width=True
            )
        
        with tab3:
            # Status distribution
            status_counts = transactions_df['status'].value_counts().reset_index()
            status_counts.columns = ['Status', 'Count']
            
            fig = px.pie(status_counts, values='Count', names='Status',
                        title='Transaction Status Distribution')
            st.plotly_chart(fig, use_container_width=True)
    
    def run_dashboard(self):
        """Main dashboard function"""
        # Header
        st.markdown('<h1 class="main-header">Business Development Performance Dashboard</h1>', 
                   unsafe_allow_html=True)
        st.markdown("---")
        
        # Sidebar for additional filters
        with st.sidebar:
            st.markdown("### 🔧 Filters & Options")
            
            # Data refresh
            if st.button("🔄 Refresh Data", use_container_width=True):
                st.rerun()
            
            st.markdown("---")
            
            # Additional filters
            st.markdown("#### Additional Filters")
            
            show_raw_data = st.checkbox("Show Raw Data", value=False)
            
            # Product category filter
            categories = list(self.product_categories.keys())
            selected_categories = st.multiselect(
                "Filter by Product Category",
                categories,
                default=categories
            )
        
        # Date range selection
        st.markdown('<h2 class="sub-header">📅 Select Analysis Period</h2>', 
                   unsafe_allow_html=True)
        
        start_date, end_date, period_type = self.create_period_selector()
        
        st.markdown("---")
        
        # Load data with progress indicator
        with st.spinner("Loading data from database..."):
            transactions_df = self.load_transaction_data(start_date, end_date)
            onboarding_df = self.load_onboarding_data(start_date, end_date)
        
        # Check if data is loaded
        if transactions_df.empty and onboarding_df.empty:
            st.warning("No data found for the selected period. Please adjust your date range.")
            return
        
        # Display metrics in tabs
        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 Executive Overview", 
            "👥 Customer Acquisition", 
            "📈 Product Analysis",
            "💰 Transactions"
        ])
        
        with tab1:
            # Calculate executive metrics
            exec_metrics = self.calculate_executive_snapshot(
                transactions_df, onboarding_df, period_type
            )
            self.display_executive_snapshot(exec_metrics)
        
        with tab2:
            # Calculate acquisition metrics
            acquisition_metrics = self.calculate_customer_acquisition(
                transactions_df, onboarding_df
            )
            self.display_customer_acquisition(acquisition_metrics)
        
        with tab3:
            # Calculate product metrics
            product_metrics = self.calculate_product_metrics(transactions_df)
            self.display_product_analysis(product_metrics)
        
        with tab4:
            self.display_transaction_analysis(transactions_df)
        
        # Raw data view
        if show_raw_data:
            st.markdown("---")
            st.markdown("### 📁 Raw Data View")
            
            data_tab1, data_tab2 = st.tabs(["Transactions", "Onboarding"])
            
            with data_tab1:
                if not transactions_df.empty:
                    st.dataframe(transactions_df, use_container_width=True)
                else:
                    st.info("No transaction data available")
            
            with data_tab2:
                if not onboarding_df.empty:
                    st.dataframe(onboarding_df, use_container_width=True)
                else:
                    st.info("No onboarding data available")
        
        # Footer
        st.markdown("---")
        st.markdown(
            """
            <div style='text-align: center; color: #6B7280; font-size: 0.9rem;'>
                <p>Business Development Performance Dashboard • Data updated in real-time</p>
                <p>For support, contact: analytics@company.com</p>
            </div>
            """,
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
        st.error(f"An error occurred: {str(e)}")
        st.info("Please check your database connection and try again.")

if __name__ == "__main__":
    main()
