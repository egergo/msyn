
This is FETCHING!

RealmFetches

| PartitionKey | empty |
| RowKey | name of the realm in region-slug format, e.g. eu-mazrigos |
| LastModified | detected last modified date |
| LastFetched | date of last fetched |
| LockedUntil | the realm is now being processed and should not be processed before this date |
| Region | region |
| Realm | slug |
| Enabled | if the realm should be fetched |
| URL | url of the data |

++ current processed timestamp (LastProcessed)



XAuctions20150808 - daily auction data
Partition: items-eu-mazrigos-timestamp



Auctions

Auctions-{region}-{realm}-{timestamp}

Partition: items
RowKey: itemId
Auctions: json string of [auction]

Partition: owners
RowKey: owner name
Auctions: ids? all auctions?