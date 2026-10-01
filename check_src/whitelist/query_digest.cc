>   my_h_string digest;
>   string_factory_srv->create(&digest);
> 
<   /* PXC: Cherrypicked commit from PS 26, to be removed once code is propagated
<      to PS 9.7. */
<   /*
<    * The "query_digest" THD attribute allocates a new string and stores it
<    * in the provided handle, it must not be pre-created to avoid a leak.
<    */
<   my_h_string digest = nullptr;
< 
<                           reinterpret_cast<void *>(&digest)) &&
<       digest != nullptr) {
>                           reinterpret_cast<void *>(&digest))) {
