-- Source OLTP schema for a multi-clinic booking app. All data is synthetic.
-- REPLICA IDENTITY FULL makes Debezium emit the full old row on UPDATE/DELETE,
-- which the lakehouse needs to keep complete history.

CREATE TABLE clinics (
    clinic_id    SERIAL PRIMARY KEY,
    clinic_name  TEXT        NOT NULL,
    city         TEXT        NOT NULL,
    province     TEXT        NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE practitioners (
    practitioner_id SERIAL PRIMARY KEY,
    clinic_id       INT         NOT NULL REFERENCES clinics (clinic_id),
    full_name       TEXT        NOT NULL,
    discipline      TEXT        NOT NULL,   -- physiotherapy, massage, chiropractic, counselling
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE patients (
    patient_id  SERIAL PRIMARY KEY,
    full_name   TEXT        NOT NULL,
    email       TEXT,
    phone       TEXT,
    city        TEXT,
    birth_year  INT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE appointments (
    appointment_id  SERIAL PRIMARY KEY,
    clinic_id       INT           NOT NULL REFERENCES clinics (clinic_id),
    practitioner_id INT           NOT NULL REFERENCES practitioners (practitioner_id),
    patient_id      INT           NOT NULL REFERENCES patients (patient_id),
    service_type    TEXT          NOT NULL,
    starts_at       TIMESTAMPTZ   NOT NULL,
    ends_at         TIMESTAMPTZ   NOT NULL,   -- deliberately no CHECK: lets bad rows reach the quality layer
    status          TEXT          NOT NULL,   -- booked, completed, cancelled, no_show
    price           NUMERIC(10,2) NOT NULL,
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE payments (
    payment_id      SERIAL PRIMARY KEY,
    appointment_id  INT           NOT NULL REFERENCES appointments (appointment_id),
    amount          NUMERIC(10,2) NOT NULL,   -- no CHECK on purpose (see above)
    method          TEXT          NOT NULL,   -- card, insurance, cash
    status          TEXT          NOT NULL,   -- captured, refunded
    paid_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);

ALTER TABLE clinics       REPLICA IDENTITY FULL;
ALTER TABLE practitioners REPLICA IDENTITY FULL;
ALTER TABLE patients      REPLICA IDENTITY FULL;
ALTER TABLE appointments  REPLICA IDENTITY FULL;
ALTER TABLE payments      REPLICA IDENTITY FULL;

-- Publication used by Debezium's pgoutput plugin.
CREATE PUBLICATION clinic_cdc FOR TABLE clinics, practitioners, patients, appointments, payments;

-- Small seed so the snapshot phase has something to read.
INSERT INTO clinics (clinic_name, city, province) VALUES
    ('Fraser Valley Physio', 'Surrey', 'BC'),
    ('Kits Movement Clinic', 'Vancouver', 'BC'),
    ('Bow River Wellness', 'Calgary', 'AB');

INSERT INTO practitioners (clinic_id, full_name, discipline) VALUES
    (1, 'Priya Sandhu', 'physiotherapy'),
    (1, 'Tom Becker', 'massage'),
    (2, 'Ana Ruiz', 'chiropractic'),
    (2, 'Liam Chen', 'physiotherapy'),
    (3, 'Grace Okafor', 'counselling');
