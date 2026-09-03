-- op: ddl_person_student
-- dialect: sqlite
CREATE TABLE IF NOT EXISTS "people" (
  "id" BIGINT NOT NULL,
    "name" TEXT NOT NULL,
  PRIMARY KEY ("id")
);
CREATE TABLE IF NOT EXISTS "students" (
  "id" BIGINT NOT NULL,
    "grade" TEXT NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "fk_students_parent" FOREIGN KEY ("id") REFERENCES "people"("id") ON DELETE CASCADE
)
-- params: []
