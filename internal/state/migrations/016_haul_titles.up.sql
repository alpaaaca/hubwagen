-- What each build says about its hauls, for people: a title ("Daily
-- revenue") and a description. The newest build that defines a haul decides.
ALTER TABLE build_hauls ADD COLUMN title TEXT NOT NULL DEFAULT '';
ALTER TABLE build_hauls ADD COLUMN description TEXT NOT NULL DEFAULT '';
