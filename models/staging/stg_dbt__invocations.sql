with base as (

    select *
    from {{ source('dbt_artifacts', 'invocations') }}

    {% if is_incremental() %}
      -- this filter will only be applied on an incremental run
      where run_started_at > (select IFNULL(max(run_started_at), TIMESTAMP("1900-01-01")) from {{ this }})
    {% endif %}

),

enhanced as (

    select
        command_invocation_id,
        dbt_version,
        project_name,
        run_started_at,
        dbt_command,
        full_refresh_flag,
        target_profile_name,
        target_name,
        target_schema,
        target_threads,
        dbt_cloud_project_id,
        dbt_cloud_job_id,
        dbt_cloud_run_id,
        dbt_cloud_run_reason_category,
        dbt_cloud_run_reason,
        env_vars,
        dbt_vars
    from base

)

select * from enhanced
