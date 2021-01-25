--
-- PostgreSQL database dump
--

-- Dumped from database version 12.3
-- Dumped by pg_dump version 12.2

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: avocado; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.avocado (
    state text,
    date_used text,
    "timestamp" timestamp without time zone,
    fetch_timestamp timestamp without time zone,
    date text,
    positive integer,
    "positiveCasesViral" integer,
    "probableCases" integer,
    death integer,
    "deathConfirmed" integer,
    "deathProbable" integer,
    total integer,
    "totalTestsAntibody" integer,
    "positiveTestsAntibody" integer,
    "negativeTestsAntibody" integer,
    "totalTestsViral" integer,
    "positiveTestsViral" integer,
    "negativeTestsViral" integer,
    "totalTestEncountersViral" integer,
    "totalTestsAntigen" integer,
    "positiveTestsAntigen" integer,
    "negativeTestsAntigen" integer
);


--
-- Name: ix_avocado_by_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_avocado_by_date ON public.avocado USING btree (date_used);


--
-- Name: ix_avocado_state; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_avocado_state ON public.avocado USING btree (state);


--
-- Name: ix_avocado_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_avocado_timestamp ON public.avocado USING btree ("timestamp");


--
-- PostgreSQL database dump complete
--

