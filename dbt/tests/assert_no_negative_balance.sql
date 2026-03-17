-- Custom test: No active account should have a negative balance
-- This is a singular test (placed in /tests/ directory)

select
    account_id,
    account_number,
    balance,
    account_status
from {{ ref('stg_accounts') }}
where balance < 0
  and account_status = 'active'
