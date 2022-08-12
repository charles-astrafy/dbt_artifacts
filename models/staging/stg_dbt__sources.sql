with base as (

    select *
    from {{ source('dbt_artifacts', 'sources') }}

    {% if is_incremental() %}
      -- this filter will only be applied on an incremental run
      where run_started_at > (select IFNULL(max(run_started_at), TIMESTAMP("1900-01-01")) from {{ this }})
    {% endif %}
),

enhanced as (

    select
        {{ dbt_utils.surrogate_key(['command_invocation_id', 'node_id']) }} as source_execution_id,
        command_invocation_id,
        node_id,
        run_started_at,
        database,
        schema,
        source_name,
        loader,
        name,
        identifier,
        loaded_at_field,
        freshness
    from base

)

select * from enhanced
