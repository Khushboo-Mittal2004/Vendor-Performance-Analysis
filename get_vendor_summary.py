import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db
logging.basicconfig(
    filename="logs/get_vendor_summary.log",
    level=logging.debug,
    format="%(asctime)s-%(levelname)s-%(message)s",
    filemode="a"
)
def create_vendor_summary(conn):
    '''this function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data'''
    vendor_sales_summary=pd.tread_sql_query("""with freightsummary as (
        select
            vendornumber,
            sum(freight) as freightcost,
        from vendor_invoice
        group by vendornumber
    ),
    purchasesummary as (
        select
            p.vendornumber,
            p.vendorname,
            p.brand,
            p.description,
            p.purchaseprice,
            pp.price s actualprice,
            pp.volume,
            sum(p.quantity) as totalpurchasequantity,
            sum(p.dollars) as totalpurchasedollars
        from purchases p
        join purchase_prices pp
            on p.brand=pp.brand
        where p.purchaseprice>0
        group by p.vendornumber,p.vendorname,p.brand,p.description,p.purchaseprice,pp.price,pp.volume
    ),
    salessummary as (
        select
            vendorno,
            brand,
            sum(salesquantity) as totalsalesquantity,
            sum(salesdollars) as totalsalesdollars,
            sum(salesprice) as totalsalesprice,
            sum(excisetax) as totalexcisetax
        from sales
        group by vendorno, brand
    )
    select 
        ps.vendornumber,
        ps.vendorname,
        ps.brand,
        ps.description,
        ps.purchaseprice,
        ps.actualprice,
        ps.volume,
        ps.totalpurchasequantity,
        ps.totalpurchasedollars,
        ss.totalsalesquantity,
        ss.totalsalesdollars,
        ss.totalsalesprice,
        ss.totalexcisetax,
        fs.freightcost
    from purchasesummary ps
    left join salessummary ss
        on ps.vendornumber=ss.vendorno
        and ps.brand=ss.brand
    left join freightsummary fs
        on ps.vendornumber=fs.vendornumber
    order by ps.totalpurchasedollars desc""",conn)
    return vendor_sales_summary


def clean_data(df):
    '''this function will clean the data'''
    df['volume']=df['volume'].astype('float')
    df.fillna(0,inplace=True)
    df['vendorname']=df['vendorname'].str.strip()
    df['description']=df['description'].str.strip()
    vendor_sales_summary['grossprofit']=vendor_sales_summary['totalsalesdollars']-vendor_sales_summary['totalpurchasedollars']
    vendor_sales_summary['profitmargin']=(vendor_sales_summary['grossprofit']/vendor_sales_summary['totalsalesdollars'])*100
    vendor_sales_summary['stockturnover']=vendor_sales_summary['totalsalesquantity']/vendor_sales_summary['totalpurchasequantity']
                                    vendor_sales_summary['salestopurchaseratio']=vendor_sales_summary['totalsalesdollars']/vendor_sales_summary['totalpurchasedollars']

    return df


if __name__=='__main__':
    conn=sqlite3.connect('inventory.db')
    logging.info('creating vendor summary table...')
    summary_df=create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('cleaning data....')
    clean_df=clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting data...')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logging.info('completed')