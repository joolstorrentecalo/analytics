{% docs atr_change_flag %}

Field showing movement of the top subscriptions by arr each month. Available values - 'Became available to renew', 'Dropped as available to renew', 'New subscription', 'Subscription ends this month' or 'Top available to renew excluded deal'. These are calculated as shown below:

### Top available to renew excluded deal

This flags subscriptions where the `is_available_to_renew` flag is false. The top 10 by arr are shown each month.

### Subscription ends this month

Either the multi_year_booking_subscription_end_month or bookings_term_end_month is in the month being checked.

### Became available to renew

This model uses the daily snapshot of mart_available_to_renew so by using the LAG function to compare the `is_available_to_renew` flag to the previous day, we can see which went from false to true indicating that they became available to renew in that month. The top 10 by arr are shown.

### Dropped as available to renew

The inverse of the above. The `is_available_to_renew` flag went from true to false.

### New subscription

A new `subscription_name` has appeared this month, limited to top 10 by arr.

{% enddocs %}

{% docs rpt_accounting_period_balance_monthly %}

The report mirrors Zuora accounting period monthly balances.

The  following columns are included:

### Fiscal Year

### Fiscal Quarter

### Period

### Starting Accounts Receivable 
Ending Accounts Receivable from previous month

### Total Billings 
Total billed in a month including tax

### Payments
All payments received in the month regardless whether applied or not applied to invoices. The amount may vary from Zuora if payments were backdated as Zuora takes a snapshot of this information for the accounting period but if payments are made after the snapshot was taken the payments will not flow in. The variance will be in favor of GitLab

### Overpayments 
Payments that were not applied to invoices

### Refunds 
All refunds made from invoices and accounts

### Adjustments 
Invoice item adjustments made to invoices

### Ending Accounts Receivable = Starting Accounts Receivable + Total Billings - Payments minus Overpayments + Refunds - Adjustments

### Invoice Aging Buckets which are as follows:

Current - current due date open invoices balances

Further buckets: 1 to 30 days past due, 31 to 60 days past due, 61 to 90 days past due, 91 to 120 days past due, more than 120 days past due

### Total Invoice Aging Balance 
Total of all aging buckets

### Variance between Ending Accounts Receivable and Total Invoice Aging Balance

### Credit Balance (Customer Refunds) 
Credit balance adjustments running total for the month

### Payments or Refunds on Future Dated Invoices

### Final Check 
The variance between Ending Accounts Receivable and Total Invoice Aging Balance is taken less Credit Balance and Payments or Refunds on Future Dated Invoices - this should show a 0 variance however it is possible that due to backdated payment application this amount will not balance out to 0

{% enddocs %}

{% docs rpt_booking_billing_collections_monthly %}

Booking - total booking amount of booked opportunities in the month

Billing - total billing, billing exclusive the tax amount and tax amount of the invoicing in the month

Collections - total of payments applied to invoices in the month

{% enddocs %}


