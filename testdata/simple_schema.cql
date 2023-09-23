CREATE TABLE events(
	tenant_key bigint,
	user_id uuid,
	event_category text,
	event_id timeuuid,
	event_data blob,
	PRIMARY KEY ((tenant_key, user_id, event_category), event_id)
);
