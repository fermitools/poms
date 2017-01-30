

ALTER TABLE experimenters ADD last_login timestamptz DEFAULT now() NOT NULL;

