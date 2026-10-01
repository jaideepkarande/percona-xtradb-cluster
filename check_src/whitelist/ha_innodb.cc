<   /* PXC: Cherrypicked commit from PS 26, to be removed once code is propagated
<      to PS 9.7. */
<   /* The background thread check has to come first: internal THDs created for
<   the parallel DDL scan workers have their THD::variables poisoned (see
<   ddl::Parallel_cursor::scan()), so reading log_slow_verbosity out of them
<   trips AddressSanitizer. Such threads never log slow queries anyway. */
<   return thd && thd_opt_slow_log() && !thd_is_background_thread(thd) &&
<          unlikely(thd_log_slow_verbosity(thd) & (1ULL << SLOG_V_INNODB));
>   return thd && thd_opt_slow_log() &&
>          unlikely(thd_log_slow_verbosity(thd) & (1ULL << SLOG_V_INNODB)) &&
>          !thd_is_background_thread(thd);
