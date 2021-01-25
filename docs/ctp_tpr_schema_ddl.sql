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
-- Name: tpr; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tpr (
    state text,
    units text,
    "window" text,
    sid text,
    "timestamp" timestamp without time zone,
    fetch_timestamp timestamp without time zone,
    date text,
    total double precision,
    positive double precision,
    ppr text
);


--
-- Name: ix_tpr_sid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_tpr_sid ON public.tpr USING btree (sid);


--
-- Name: ix_tpr_state; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_tpr_state ON public.tpr USING btree (state);


--
-- Name: ix_tpr_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_tpr_timestamp ON public.tpr USING btree ("timestamp");


--
-- Name: ix_tpr_units; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_tpr_units ON public.tpr USING btree (units);


--
-- Name: ix_tpr_window; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_tpr_window ON public.tpr USING btree ("window");


--
-- PostgreSQL database dump complete
--

