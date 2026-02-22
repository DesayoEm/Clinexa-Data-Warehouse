{% snapshot central_contacts_snapshot %}

{{
    config(
        target_schema='<enrollment>',
        unique_key='<contact_key>',
        strategy='check,
        check_cols=['name', 'role', 'phone', 'email']
    )
}}

SELECT * FROM {{ source('staging', 'central_contacts') }}
{% endsnapshot %}