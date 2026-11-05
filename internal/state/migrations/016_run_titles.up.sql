-- What each build says about its runs, for people: a title ("Daily
-- revenue") and a description. The newest build that defines a run decides.
ALTER TABLE build_pipelines ADD COLUMN title TEXT NOT NULL DEFAULT '';
ALTER TABLE build_pipelines ADD COLUMN description TEXT NOT NULL DEFAULT '';
