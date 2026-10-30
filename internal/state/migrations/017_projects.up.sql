-- Projects (hubwagen deploy): each project's current deployment, and when it
-- was deleted. Deployments themselves are events (ProjectDeployed).
CREATE TABLE projects (
    name         TEXT PRIMARY KEY,
    version      INTEGER NOT NULL,
    deployment   TEXT NOT NULL,       -- JSON: state.Deployment
    deployed_at  TEXT NOT NULL,
    deleted_at   TEXT
);
