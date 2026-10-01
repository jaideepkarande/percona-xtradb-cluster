< /**
<   Grant and revoke the external roles reported by the authentication plugin.
< 
<   @param[in] thd               Thread context
<   @param[in] plugin_roles_list comma separated roles reported by the plugin
<   @param[in] acl_user          the authenticated user
< 
<   @retval false roles applied, or nothing to do
<   @retval true  the ACL cache lock could not be acquired; an error has been
<                 raised and the caller must fail the authentication
< */
< static bool apply_external_roles(THD *thd, const char *plugin_roles_list,
> static void apply_external_roles(THD *thd, const char *plugin_roles_list,
<   if (acl_user->user == nullptr) return false;
>   if (acl_user->user == nullptr) return;
<   {
<     Acl_cache_lock_guard acl_cache_lock(thd, Acl_cache_lock_mode::READ_MODE);
<     bool locked = acl_cache_lock.lock();
<     DBUG_EXECUTE_IF("fail_acl_external_roles_read_lock", {
<       if (locked) {
<         acl_cache_lock.unlock();
<         // Acl_cache_lock_guard::lock() calls my_error() on failure.
<         // Emulate it here so the diagnostics area is populated.
<         my_error(ER_CANNOT_LOCK_USER_MANAGEMENT_CACHES, MYF(0));
<         locked = false;
<       }
<     });
<     if (!locked) return true;
<     DBUG_EXECUTE_IF("acl_external_roles_log_lock_mode", {
<       LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
<              thd->mdl_context.owns_equal_or_stronger_lock(MDL_key::ACL_CACHE,
<                                                           "", "", MDL_EXCLUSIVE)
<                  ? "PS-11593 external roles probe lock mode: EXCLUSIVE"
<                  : "PS-11593 external roles probe lock mode: SHARED");
<     });
<     if (plugin_roles.empty() &&
<         g_external_roles.find(user) == g_external_roles.end())
<       return false;
<   }  // the READ lock is released here, it is never upgraded
< 
<   bool write_locked = acl_cache_lock_guard.lock();
<   DBUG_EXECUTE_IF("fail_acl_external_roles_write_lock", {
<     if (write_locked) {
<       acl_cache_lock_guard.unlock();
<       // Acl_cache_lock_guard::lock() calls my_error() on failure.
<       // Emulate it here so the diagnostics area is populated.
<       my_error(ER_CANNOT_LOCK_USER_MANAGEMENT_CACHES, MYF(0));
<       write_locked = false;
<     }
<   });
<   if (!write_locked) return true;
<   // The lookup is repeated: no lock was held between the two acquisitions, so
<   // the container may have changed in between.
>   acl_cache_lock_guard.lock();
<   if (user_roles_it == g_external_roles.end() && plugin_roles.empty())
<     return false;
>   if (user_roles_it == g_external_roles.end() && plugin_roles.empty()) return;
<   if (cached_acl_user == nullptr) return false;
>   if (cached_acl_user == nullptr) return;
< 
<   return false;
<       // A failed ACL cache lock has already raised an error; the login must
<       // fail here rather than reach my_ok() with an error in the DA.
<       if (apply_external_roles(thd, mpvio.auth_info.external_roles, acl_user))
<         goto end;
>       apply_external_roles(thd, mpvio.auth_info.external_roles, acl_user);
