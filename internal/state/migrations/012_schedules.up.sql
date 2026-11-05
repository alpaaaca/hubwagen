-- Run schedules: the newest build defining a run decides its schedule;
-- the leader keeps a cursor per schedule (see internal/schedule).
CREATE TABLE schedules (
    tenant      TEXT NOT NULL,
    run        TEXT NOT NULL,
    cron        TEXT NOT NULL,
    timezone    TEXT NOT NULL DEFAULT '',
    interval    TEXT NOT NULL,              -- hourly|daily|weekly|monthly|cron
    catch_up    TEXT NOT NULL,              -- skip|latest|all
    build_id    TEXT NOT NULL,              -- the build it came from
    cursor      TEXT NOT NULL,              -- end of the last interval handled (cron: last tick)
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (tenant, run)
);
