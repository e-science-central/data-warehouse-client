--
-- PostgreSQL database dump
--

-- Dumped from database version 13.7
-- Dumped by pg_dump version 15.2

-- Started on 2023-03-28 14:53:03

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

--
-- TOC entry 6 (class 2615 OID 2200)
-- Name: public; Type: SCHEMA; Schema: -; Owner: osm_dw
--

-- *not* creating schema, since initdb creates it


ALTER SCHEMA public OWNER TO osm_dw;

--
-- TOC entry 2 (class 3079 OID 16404)
-- Name: tablefunc; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS tablefunc WITH SCHEMA public;


--
-- TOC entry 4046 (class 0 OID 0)
-- Dependencies: 2
-- Name: EXTENSION tablefunc; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION tablefunc IS 'functions that manipulate whole tables, including crosstab';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 204 (class 1259 OID 16425)
-- Name: measurementgroup; Type: TABLE; Schema: public; Owner: osm_dw
--

CREATE TABLE public.measurementgroup (
    id integer NOT NULL,
    description character varying(100),
    study integer NOT NULL
);


ALTER TABLE public.measurementgroup OWNER TO osm_dw;

--
-- TOC entry 205 (class 1259 OID 16428)
-- Name: MeasurementGroup_Id_seq; Type: SEQUENCE; Schema: public; Owner: osm_dw
--

CREATE SEQUENCE public."MeasurementGroup_Id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public."MeasurementGroup_Id_seq" OWNER TO osm_dw;

--
-- TOC entry 4059 (class 0 OID 0)
-- Dependencies: 205
-- Name: MeasurementGroup_Id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: osm_dw
--

ALTER SEQUENCE public."MeasurementGroup_Id_seq" OWNED BY public.measurementgroup.id;


--
-- TOC entry 206 (class 1259 OID 16430)
-- Name: measurementtype; Type: TABLE; Schema: public; Owner: osm_dw
--

CREATE TABLE public.measurementtype (
    id integer NOT NULL,
    description character varying(300),
    valtype integer,
    units integer,
    study integer NOT NULL
);


ALTER TABLE public.measurementtype OWNER TO osm_dw;

--
-- TOC entry 207 (class 1259 OID 16433)
-- Name: MeasurementType_Id_seq; Type: SEQUENCE; Schema: public; Owner: osm_dw
--

CREATE SEQUENCE public."MeasurementType_Id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public."MeasurementType_Id_seq" OWNER TO osm_dw;

--
-- TOC entry 4062 (class 0 OID 0)
-- Dependencies: 207
-- Name: MeasurementType_Id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: osm_dw
--

ALTER SEQUENCE public."MeasurementType_Id_seq" OWNED BY public.measurementtype.id;


--
-- TOC entry 208 (class 1259 OID 16435)
-- Name: measurement; Type: TABLE; Schema: public; Owner: osm_dw
--

CREATE TABLE public.measurement (
    id bigint NOT NULL,
    groupinstance bigint NOT NULL,
    measurementtype integer NOT NULL,
    participant integer,
    study integer NOT NULL,
    source bigint,
    valtype integer NOT NULL,
    valinteger integer,
    valreal double precision,
    "time" timestamp without time zone NOT NULL,
    measurementgroup integer,
    trial integer
);


ALTER TABLE public.measurement OWNER TO osm_dw;

--
-- TOC entry 209 (class 1259 OID 16438)
-- Name: Measurement_Id_seq; Type: SEQUENCE; Schema: public; Owner: osm_dw
--

CREATE SEQUENCE public."Measurement_Id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public."Measurement_Id_seq" OWNER TO osm_dw;

--
-- TOC entry 4065 (class 0 OID 0)
-- Dependencies: 209
-- Name: Measurement_Id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: osm_dw
--

ALTER SEQUENCE public."Measurement_Id_seq" OWNED BY public.measurement.id;


--
-- TOC entry 210 (class 1259 OID 16440)
-- Name: participant; Type: TABLE; Schema: public; Owner: osm_dw
--

CREATE TABLE public.participant (
    id integer NOT NULL,
    participantid character varying(40),
    study integer NOT NULL
);


ALTER TABLE public.participant OWNER TO osm_dw;

--
-- TOC entry 211 (class 1259 OID 16443)
-- Name: Participant_Id_seq; Type: SEQUENCE; Schema: public; Owner: osm_dw
--

CREATE SEQUENCE public."Participant_Id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public."Participant_Id_seq" OWNER TO osm_dw;

--
-- TOC entry 4068 (class 0 OID 0)
-- Dependencies: 211
-- Name: Participant_Id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: osm_dw
--

ALTER SEQUENCE public."Participant_Id_seq" OWNED BY public.participant.id;


--
-- TOC entry 212 (class 1259 OID 16445)
-- Name: sourcetype; Type: TABLE; Schema: public; Owner: osm_dw
--

CREATE TABLE public.sourcetype (
    id integer NOT NULL,
    description character varying(500),
    version character varying(20),
    study integer NOT NULL
);


ALTER TABLE public.sourcetype OWNER TO osm_dw;

--
-- TOC entry 213 (class 1259 OID 16451)
-- Name: SourceType_Id_seq; Type: SEQUENCE; Schema: public; Owner: osm_dw
--

CREATE SEQUENCE public."SourceType_Id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public."SourceType_Id_seq" OWNER TO osm_dw;

--
-- TOC entry 4071 (class 0 OID 0)
-- Dependencies: 213
-- Name: SourceType_Id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: osm_dw
--

ALTER SEQUENCE public."SourceType_Id_seq" OWNED BY public.sourcetype.id;


--
-- TOC entry 214 (class 1259 OID 16453)
-- Name: source; Type: TABLE; Schema: public; Owner: osm_dw
--

CREATE TABLE public.source (
    id bigint NOT NULL,
    sourceid character varying,
    sourcetype integer NOT NULL,
    study integer NOT NULL
);


ALTER TABLE public.source OWNER TO osm_dw;

--
-- TOC entry 215 (class 1259 OID 16459)
-- Name: Source_Id_seq; Type: SEQUENCE; Schema: public; Owner: osm_dw
--

CREATE SEQUENCE public."Source_Id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public."Source_Id_seq" OWNER TO osm_dw;

--
-- TOC entry 4074 (class 0 OID 0)
-- Dependencies: 215
-- Name: Source_Id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: osm_dw
--

ALTER SEQUENCE public."Source_Id_seq" OWNED BY public.source.id;


--
-- TOC entry 216 (class 1259 OID 16461)
-- Name: study; Type: TABLE; Schema: public; Owner: osm_dw
--

CREATE TABLE public.study (
    id integer NOT NULL,
    studyid character varying(500)
);


ALTER TABLE public.study OWNER TO osm_dw;

--
-- TOC entry 217 (class 1259 OID 16464)
-- Name: Study_Id_seq; Type: SEQUENCE; Schema: public; Owner: osm_dw
--

CREATE SEQUENCE public."Study_Id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public."Study_Id_seq" OWNER TO osm_dw;

--
-- TOC entry 4077 (class 0 OID 0)
-- Dependencies: 217
-- Name: Study_Id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: osm_dw
--

ALTER SEQUENCE public."Study_Id_seq" OWNED BY public.study.id;


--
-- TOC entry 226 (class 1259 OID 16755)
-- Name: boundsdatetime; Type: TABLE; Schema: public; Owner: osm_dw
--

CREATE TABLE public.boundsdatetime (
    measurementtype integer NOT NULL,
    study integer NOT NULL,
    minval timestamp without time zone NOT NULL,
    maxval timestamp without time zone NOT NULL
);


ALTER TABLE public.boundsdatetime OWNER TO osm_dw;

--
-- TOC entry 218 (class 1259 OID 16466)
-- Name: boundsint; Type: TABLE; Schema: public; Owner: osm_dw
--

CREATE TABLE public.boundsint (
    measurementtype integer NOT NULL,
    minval integer NOT NULL,
    maxval integer NOT NULL,
    study integer NOT NULL
);


ALTER TABLE public.boundsint OWNER TO osm_dw;

--
-- TOC entry 219 (class 1259 OID 16469)
-- Name: boundsreal; Type: TABLE; Schema: public; Owner: osm_dw
--

CREATE TABLE public.boundsreal (
    measurementtype integer NOT NULL,
    minval double precision NOT NULL,
    maxval double precision NOT NULL,
    study integer NOT NULL
);


ALTER TABLE public.boundsreal OWNER TO osm_dw;

--
-- TOC entry 220 (class 1259 OID 16472)
-- Name: category; Type: TABLE; Schema: public; Owner: osm_dw
--

CREATE TABLE public.category (
    measurementtype integer NOT NULL,
    categoryid integer NOT NULL,
    categoryname character varying(150),
    study integer NOT NULL
);


ALTER TABLE public.category OWNER TO osm_dw;

--
-- TOC entry 221 (class 1259 OID 16475)
-- Name: datetimevalue; Type: TABLE; Schema: public; Owner: osm_dw
--

CREATE TABLE public.datetimevalue (
    measurement bigint NOT NULL,
    datetimeval timestamp without time zone NOT NULL,
    study integer NOT NULL
);


ALTER TABLE public.datetimevalue OWNER TO osm_dw;

--
-- TOC entry 222 (class 1259 OID 16486)
-- Name: measurementtypetogroup; Type: TABLE; Schema: public; Owner: osm_dw
--

CREATE TABLE public.measurementtypetogroup (
    measurementtype integer NOT NULL,
    measurementgroup integer NOT NULL,
    name character varying(300),
    study integer NOT NULL,
    optional boolean
);


ALTER TABLE public.measurementtypetogroup OWNER TO osm_dw;

--
-- TOC entry 223 (class 1259 OID 16489)
-- Name: textvalue; Type: TABLE; Schema: public; Owner: osm_dw
--

CREATE TABLE public.textvalue (
    measurement bigint NOT NULL,
    textval character varying(2000) NOT NULL,
    study integer NOT NULL
);


ALTER TABLE public.textvalue OWNER TO osm_dw;

--
-- TOC entry 224 (class 1259 OID 16495)
-- Name: trial; Type: TABLE; Schema: public; Owner: osm_dw
--

CREATE TABLE public.trial (
    id integer NOT NULL,
    study integer NOT NULL,
    description character varying(500)
);


ALTER TABLE public.trial OWNER TO osm_dw;

--
-- TOC entry 225 (class 1259 OID 16501)
-- Name: units; Type: TABLE; Schema: public; Owner: osm_dw
--

CREATE TABLE public.units (
    id integer NOT NULL,
    name character varying(20) NOT NULL,
    study integer NOT NULL
);


ALTER TABLE public.units OWNER TO osm_dw;

--
-- TOC entry 3812 (class 2604 OID 16505)
-- Name: measurement id; Type: DEFAULT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurement ALTER COLUMN id SET DEFAULT nextval('public."Measurement_Id_seq"'::regclass);


--
-- TOC entry 3810 (class 2604 OID 16506)
-- Name: measurementgroup id; Type: DEFAULT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurementgroup ALTER COLUMN id SET DEFAULT nextval('public."MeasurementGroup_Id_seq"'::regclass);


--
-- TOC entry 3811 (class 2604 OID 16507)
-- Name: measurementtype id; Type: DEFAULT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurementtype ALTER COLUMN id SET DEFAULT nextval('public."MeasurementType_Id_seq"'::regclass);


--
-- TOC entry 3813 (class 2604 OID 16508)
-- Name: participant id; Type: DEFAULT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.participant ALTER COLUMN id SET DEFAULT nextval('public."Participant_Id_seq"'::regclass);


--
-- TOC entry 3815 (class 2604 OID 16509)
-- Name: source id; Type: DEFAULT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.source ALTER COLUMN id SET DEFAULT nextval('public."Source_Id_seq"'::regclass);


--
-- TOC entry 3814 (class 2604 OID 16510)
-- Name: sourcetype id; Type: DEFAULT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.sourcetype ALTER COLUMN id SET DEFAULT nextval('public."SourceType_Id_seq"'::regclass);


--
-- TOC entry 3816 (class 2604 OID 16511)
-- Name: study id; Type: DEFAULT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.study ALTER COLUMN id SET DEFAULT nextval('public."Study_Id_seq"'::regclass);


--
-- TOC entry 3848 (class 2606 OID 16513)
-- Name: study Study_pkey; Type: CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.study
    ADD CONSTRAINT "Study_pkey" PRIMARY KEY (id);


--
-- TOC entry 3877 (class 2606 OID 16759)
-- Name: boundsdatetime boundsdatetime_pkey; Type: CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.boundsdatetime
    ADD CONSTRAINT boundsdatetime_pkey PRIMARY KEY (measurementtype, study);


--
-- TOC entry 3857 (class 2606 OID 16515)
-- Name: category category_pkey; Type: CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.category
    ADD CONSTRAINT category_pkey PRIMARY KEY (measurementtype, categoryid, study);


--
-- TOC entry 3852 (class 2606 OID 16519)
-- Name: boundsint pk_boundsint; Type: CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.boundsint
    ADD CONSTRAINT pk_boundsint PRIMARY KEY (measurementtype, study);


--
-- TOC entry 3855 (class 2606 OID 16521)
-- Name: boundsreal pk_boundsreal; Type: CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.boundsreal
    ADD CONSTRAINT pk_boundsreal PRIMARY KEY (measurementtype, study);


--
-- TOC entry 3863 (class 2606 OID 16523)
-- Name: datetimevalue pk_datetimevalue; Type: CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.datetimevalue
    ADD CONSTRAINT pk_datetimevalue PRIMARY KEY (measurement, study);


--
-- TOC entry 3836 (class 2606 OID 16525)
-- Name: measurement pk_measurement; Type: CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT pk_measurement PRIMARY KEY (id, study);


--
-- TOC entry 3819 (class 2606 OID 16527)
-- Name: measurementgroup pk_measurementgroup; Type: CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurementgroup
    ADD CONSTRAINT pk_measurementgroup PRIMARY KEY (id, study);


--
-- TOC entry 3824 (class 2606 OID 16529)
-- Name: measurementtype pk_measurementtype; Type: CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurementtype
    ADD CONSTRAINT pk_measurementtype PRIMARY KEY (id, study);


--
-- TOC entry 3868 (class 2606 OID 16531)
-- Name: measurementtypetogroup pk_measurementtypetogroup; Type: CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurementtypetogroup
    ADD CONSTRAINT pk_measurementtypetogroup PRIMARY KEY (measurementtype, measurementgroup, study);


--
-- TOC entry 3839 (class 2606 OID 16533)
-- Name: participant pk_participant; Type: CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.participant
    ADD CONSTRAINT pk_participant PRIMARY KEY (id, study);


--
-- TOC entry 3846 (class 2606 OID 16535)
-- Name: source pk_source; Type: CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.source
    ADD CONSTRAINT pk_source PRIMARY KEY (id, study);


--
-- TOC entry 3842 (class 2606 OID 16537)
-- Name: sourcetype pk_sourcetype; Type: CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.sourcetype
    ADD CONSTRAINT pk_sourcetype PRIMARY KEY (id, study);


--
-- TOC entry 3871 (class 2606 OID 16539)
-- Name: textvalue pk_textvalue; Type: CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.textvalue
    ADD CONSTRAINT pk_textvalue PRIMARY KEY (measurement, study);


--
-- TOC entry 3873 (class 2606 OID 16541)
-- Name: trial pk_trial; Type: CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.trial
    ADD CONSTRAINT pk_trial PRIMARY KEY (id, study);


--
-- TOC entry 3875 (class 2606 OID 16543)
-- Name: units pk_units; Type: CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.units
    ADD CONSTRAINT pk_units PRIMARY KEY (id, study);


--
-- TOC entry 3853 (class 1259 OID 16744)
-- Name: fki-fk-boundsreal-measurementtype; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "fki-fk-boundsreal-measurementtype" ON public.boundsreal USING btree (measurementtype, study);


--
-- TOC entry 3850 (class 1259 OID 16743)
-- Name: fki_fk-boundsint-measurementtype; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "fki_fk-boundsint-measurementtype" ON public.boundsint USING btree (measurementtype, study);


--
-- TOC entry 3878 (class 1259 OID 16770)
-- Name: fki_fk_datetime_study; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX fki_fk_datetime_study ON public.boundsdatetime USING btree (study);


--
-- TOC entry 3864 (class 1259 OID 16544)
-- Name: fki_fk_measurementgroup; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX fki_fk_measurementgroup ON public.measurementtypetogroup USING btree (measurementgroup, study);


--
-- TOC entry 3825 (class 1259 OID 16545)
-- Name: fki_fk_measurementtype; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX fki_fk_measurementtype ON public.measurement USING btree (measurementtype, study);


--
-- TOC entry 3826 (class 1259 OID 16546)
-- Name: fki_fk_measurementtypetogroup; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX fki_fk_measurementtypetogroup ON public.measurement USING btree (measurementtype, measurementgroup, study);


--
-- TOC entry 3827 (class 1259 OID 16547)
-- Name: fki_fk_participant; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX fki_fk_participant ON public.measurement USING btree (participant, study);


--
-- TOC entry 3828 (class 1259 OID 16548)
-- Name: fki_fk_source; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX fki_fk_source ON public.measurement USING btree (source, study);


--
-- TOC entry 3843 (class 1259 OID 16549)
-- Name: fki_fk_sourcetype; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX fki_fk_sourcetype ON public.source USING btree (sourcetype, study);


--
-- TOC entry 3858 (class 1259 OID 16550)
-- Name: fki_fk_study; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX fki_fk_study ON public.category USING btree (study);


--
-- TOC entry 3820 (class 1259 OID 16551)
-- Name: fki_fk_units; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX fki_fk_units ON public.measurementtype USING btree (units, study);


--
-- TOC entry 3829 (class 1259 OID 16552)
-- Name: fki_fromSOurce; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "fki_fromSOurce" ON public.measurement USING btree (source);


--
-- TOC entry 3865 (class 1259 OID 16553)
-- Name: fki_hasMeasurementGroup; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "fki_hasMeasurementGroup" ON public.measurementtypetogroup USING btree (measurementgroup);


--
-- TOC entry 3866 (class 1259 OID 16554)
-- Name: fki_hasMeasurementType; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "fki_hasMeasurementType" ON public.measurementtypetogroup USING btree (measurementtype);


--
-- TOC entry 3821 (class 1259 OID 16555)
-- Name: fki_hasUnits; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "fki_hasUnits" ON public.measurementtype USING btree (units);


--
-- TOC entry 3830 (class 1259 OID 16556)
-- Name: fki_isFromParticipant; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "fki_isFromParticipant" ON public.measurement USING btree (participant);


--
-- TOC entry 3831 (class 1259 OID 16557)
-- Name: fki_isFromStudy; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "fki_isFromStudy" ON public.measurement USING btree (study);


--
-- TOC entry 3832 (class 1259 OID 16558)
-- Name: fki_isInMeasurementGroup; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "fki_isInMeasurementGroup" ON public.measurement USING btree (measurementgroup);


--
-- TOC entry 3833 (class 1259 OID 16559)
-- Name: fki_isOfMeasurementType; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "fki_isOfMeasurementType" ON public.measurement USING btree (measurementtype);


--
-- TOC entry 3859 (class 1259 OID 16560)
-- Name: fki_measurementtype; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX fki_measurementtype ON public.category USING btree (measurementtype, study);


--
-- TOC entry 3834 (class 1259 OID 16561)
-- Name: fki_mk_trial; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX fki_mk_trial ON public.measurement USING btree (trial, study);


--
-- TOC entry 3869 (class 1259 OID 16562)
-- Name: indexDateTimeByMeasurement; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "indexDateTimeByMeasurement" ON public.textvalue USING btree (measurement);


--
-- TOC entry 3817 (class 1259 OID 16563)
-- Name: indexMeasurementGroupById; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "indexMeasurementGroupById" ON public.measurementgroup USING btree (id);


--
-- TOC entry 3860 (class 1259 OID 16564)
-- Name: indexMeasurementTypeAndId; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "indexMeasurementTypeAndId" ON public.category USING btree (measurementtype, categoryid);


--
-- TOC entry 3822 (class 1259 OID 16565)
-- Name: indexMeasurementTypeById; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "indexMeasurementTypeById" ON public.measurementtype USING btree (id);


--
-- TOC entry 3837 (class 1259 OID 16566)
-- Name: indexParticipantById; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "indexParticipantById" ON public.participant USING btree (id);


--
-- TOC entry 3844 (class 1259 OID 16567)
-- Name: indexSourceById; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "indexSourceById" ON public.source USING btree (id);


--
-- TOC entry 3840 (class 1259 OID 16568)
-- Name: indexSourceTypeById; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "indexSourceTypeById" ON public.sourcetype USING btree (id);


--
-- TOC entry 3849 (class 1259 OID 16569)
-- Name: indexStudyById; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "indexStudyById" ON public.study USING btree (id);


--
-- TOC entry 3861 (class 1259 OID 16570)
-- Name: indexTextByMeasurement; Type: INDEX; Schema: public; Owner: osm_dw
--

CREATE INDEX "indexTextByMeasurement" ON public.datetimevalue USING btree (measurement);


--
-- TOC entry 3904 (class 2606 OID 16571)
-- Name: textvalue fk-measurement; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.textvalue
    ADD CONSTRAINT "fk-measurement" FOREIGN KEY (measurement, study) REFERENCES public.measurement(id, study);


--
-- TOC entry 3895 (class 2606 OID 16576)
-- Name: boundsreal fk-measurementtype; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.boundsreal
    ADD CONSTRAINT "fk-measurementtype" FOREIGN KEY (measurementtype, study) REFERENCES public.measurementtype(id, study);


--
-- TOC entry 3893 (class 2606 OID 16581)
-- Name: boundsint fk-measurementtype; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.boundsint
    ADD CONSTRAINT "fk-measurementtype" FOREIGN KEY (measurementtype, study) REFERENCES public.measurementtype(id, study);


--
-- TOC entry 3908 (class 2606 OID 16760)
-- Name: boundsdatetime fk-measurementtype; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.boundsdatetime
    ADD CONSTRAINT "fk-measurementtype" FOREIGN KEY (measurementtype, study) REFERENCES public.measurementtype(id, study) NOT VALID;


--
-- TOC entry 3909 (class 2606 OID 16765)
-- Name: boundsdatetime fk_datetime_study; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.boundsdatetime
    ADD CONSTRAINT fk_datetime_study FOREIGN KEY (study) REFERENCES public.study(id) NOT VALID;


--
-- TOC entry 3899 (class 2606 OID 16586)
-- Name: datetimevalue fk_measurement; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.datetimevalue
    ADD CONSTRAINT fk_measurement FOREIGN KEY (measurement, study) REFERENCES public.measurement(id, study);


--
-- TOC entry 3901 (class 2606 OID 16591)
-- Name: measurementtypetogroup fk_measurementgroup; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurementtypetogroup
    ADD CONSTRAINT fk_measurementgroup FOREIGN KEY (measurementgroup, study) REFERENCES public.measurementgroup(id, study);


--
-- TOC entry 3882 (class 2606 OID 16596)
-- Name: measurement fk_measurementgroup; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fk_measurementgroup FOREIGN KEY (measurementgroup, study) REFERENCES public.measurementgroup(id, study);


--
-- TOC entry 3897 (class 2606 OID 16601)
-- Name: category fk_measurementtype; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.category
    ADD CONSTRAINT fk_measurementtype FOREIGN KEY (measurementtype, study) REFERENCES public.measurementtype(id, study);


--
-- TOC entry 3883 (class 2606 OID 16606)
-- Name: measurement fk_measurementtype; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fk_measurementtype FOREIGN KEY (study, measurementtype) REFERENCES public.measurementtype(study, id);


--
-- TOC entry 3902 (class 2606 OID 16611)
-- Name: measurementtypetogroup fk_measurementtype; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurementtypetogroup
    ADD CONSTRAINT fk_measurementtype FOREIGN KEY (study, measurementtype) REFERENCES public.measurementtype(study, id);


--
-- TOC entry 3884 (class 2606 OID 16616)
-- Name: measurement fk_measurementtypetogroup; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fk_measurementtypetogroup FOREIGN KEY (study, measurementgroup, measurementtype) REFERENCES public.measurementtypetogroup(study, measurementgroup, measurementtype);


--
-- TOC entry 3885 (class 2606 OID 16621)
-- Name: measurement fk_participant; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fk_participant FOREIGN KEY (participant, study) REFERENCES public.participant(id, study);


--
-- TOC entry 3886 (class 2606 OID 16626)
-- Name: measurement fk_source; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fk_source FOREIGN KEY (source, study) REFERENCES public.source(id, study);


--
-- TOC entry 3891 (class 2606 OID 16631)
-- Name: source fk_sourcetype; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.source
    ADD CONSTRAINT fk_sourcetype FOREIGN KEY (sourcetype, study) REFERENCES public.sourcetype(id, study);


--
-- TOC entry 3887 (class 2606 OID 16636)
-- Name: measurement fk_study; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fk_study FOREIGN KEY (study) REFERENCES public.study(id);


--
-- TOC entry 3894 (class 2606 OID 16641)
-- Name: boundsint fk_study; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.boundsint
    ADD CONSTRAINT fk_study FOREIGN KEY (study) REFERENCES public.study(id) ON UPDATE RESTRICT ON DELETE RESTRICT;


--
-- TOC entry 3900 (class 2606 OID 16646)
-- Name: datetimevalue fk_study; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.datetimevalue
    ADD CONSTRAINT fk_study FOREIGN KEY (study) REFERENCES public.study(id) ON UPDATE RESTRICT ON DELETE RESTRICT;


--
-- TOC entry 3889 (class 2606 OID 16651)
-- Name: participant fk_study; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.participant
    ADD CONSTRAINT fk_study FOREIGN KEY (study) REFERENCES public.study(id) ON UPDATE RESTRICT ON DELETE RESTRICT;


--
-- TOC entry 3907 (class 2606 OID 16656)
-- Name: units fk_study; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.units
    ADD CONSTRAINT fk_study FOREIGN KEY (study) REFERENCES public.study(id) ON UPDATE RESTRICT ON DELETE RESTRICT;


--
-- TOC entry 3906 (class 2606 OID 16661)
-- Name: trial fk_study; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.trial
    ADD CONSTRAINT fk_study FOREIGN KEY (study) REFERENCES public.study(id) ON UPDATE RESTRICT ON DELETE RESTRICT;


--
-- TOC entry 3905 (class 2606 OID 16666)
-- Name: textvalue fk_study; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.textvalue
    ADD CONSTRAINT fk_study FOREIGN KEY (study) REFERENCES public.study(id) ON UPDATE RESTRICT ON DELETE RESTRICT;


--
-- TOC entry 3890 (class 2606 OID 16671)
-- Name: sourcetype fk_study; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.sourcetype
    ADD CONSTRAINT fk_study FOREIGN KEY (study) REFERENCES public.study(id) ON UPDATE RESTRICT ON DELETE RESTRICT;


--
-- TOC entry 3903 (class 2606 OID 16676)
-- Name: measurementtypetogroup fk_study; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurementtypetogroup
    ADD CONSTRAINT fk_study FOREIGN KEY (study) REFERENCES public.study(id) ON UPDATE RESTRICT ON DELETE RESTRICT;


--
-- TOC entry 3880 (class 2606 OID 16681)
-- Name: measurementtype fk_study; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurementtype
    ADD CONSTRAINT fk_study FOREIGN KEY (study) REFERENCES public.study(id) ON UPDATE RESTRICT ON DELETE RESTRICT;


--
-- TOC entry 3879 (class 2606 OID 16686)
-- Name: measurementgroup fk_study; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurementgroup
    ADD CONSTRAINT fk_study FOREIGN KEY (study) REFERENCES public.study(id) ON UPDATE RESTRICT ON DELETE RESTRICT;


--
-- TOC entry 3892 (class 2606 OID 16691)
-- Name: source fk_study; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.source
    ADD CONSTRAINT fk_study FOREIGN KEY (study) REFERENCES public.study(id) ON UPDATE RESTRICT ON DELETE RESTRICT;


--
-- TOC entry 3896 (class 2606 OID 16696)
-- Name: boundsreal fk_study; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.boundsreal
    ADD CONSTRAINT fk_study FOREIGN KEY (study) REFERENCES public.study(id);


--
-- TOC entry 3898 (class 2606 OID 16701)
-- Name: category fk_study; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.category
    ADD CONSTRAINT fk_study FOREIGN KEY (study) REFERENCES public.study(id);


--
-- TOC entry 3888 (class 2606 OID 16706)
-- Name: measurement fk_trial; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT fk_trial FOREIGN KEY (trial, study) REFERENCES public.trial(id, study);


--
-- TOC entry 3881 (class 2606 OID 16711)
-- Name: measurementtype fk_units; Type: FK CONSTRAINT; Schema: public; Owner: osm_dw
--

ALTER TABLE ONLY public.measurementtype
    ADD CONSTRAINT fk_units FOREIGN KEY (units, study) REFERENCES public.units(id, study);


--
-- TOC entry 4045 (class 0 OID 0)
-- Dependencies: 6
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: osm_dw
--

REVOKE USAGE ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- TOC entry 4047 (class 0 OID 0)
-- Dependencies: 227
-- Name: FUNCTION connectby(text, text, text, text, integer); Type: ACL; Schema: public; Owner: rdsadmin
--

GRANT ALL ON FUNCTION public.connectby(text, text, text, text, integer) TO "Fenland Data Warehouse Read Write";


--
-- TOC entry 4048 (class 0 OID 0)
-- Dependencies: 228
-- Name: FUNCTION connectby(text, text, text, text, integer, text); Type: ACL; Schema: public; Owner: rdsadmin
--

GRANT ALL ON FUNCTION public.connectby(text, text, text, text, integer, text) TO "Fenland Data Warehouse Read Write";


--
-- TOC entry 4049 (class 0 OID 0)
-- Dependencies: 229
-- Name: FUNCTION connectby(text, text, text, text, text, integer); Type: ACL; Schema: public; Owner: rdsadmin
--

GRANT ALL ON FUNCTION public.connectby(text, text, text, text, text, integer) TO "Fenland Data Warehouse Read Write";


--
-- TOC entry 4050 (class 0 OID 0)
-- Dependencies: 230
-- Name: FUNCTION connectby(text, text, text, text, text, integer, text); Type: ACL; Schema: public; Owner: rdsadmin
--

GRANT ALL ON FUNCTION public.connectby(text, text, text, text, text, integer, text) TO "Fenland Data Warehouse Read Write";


--
-- TOC entry 4051 (class 0 OID 0)
-- Dependencies: 232
-- Name: FUNCTION crosstab(text); Type: ACL; Schema: public; Owner: rdsadmin
--

GRANT ALL ON FUNCTION public.crosstab(text) TO "Fenland Data Warehouse Read Write";


--
-- TOC entry 4052 (class 0 OID 0)
-- Dependencies: 231
-- Name: FUNCTION crosstab(text, integer); Type: ACL; Schema: public; Owner: rdsadmin
--

GRANT ALL ON FUNCTION public.crosstab(text, integer) TO "Fenland Data Warehouse Read Write";


--
-- TOC entry 4053 (class 0 OID 0)
-- Dependencies: 233
-- Name: FUNCTION crosstab(text, text); Type: ACL; Schema: public; Owner: rdsadmin
--

GRANT ALL ON FUNCTION public.crosstab(text, text) TO "Fenland Data Warehouse Read Write";


--
-- TOC entry 4054 (class 0 OID 0)
-- Dependencies: 234
-- Name: FUNCTION crosstab2(text); Type: ACL; Schema: public; Owner: rdsadmin
--

GRANT ALL ON FUNCTION public.crosstab2(text) TO "Fenland Data Warehouse Read Write";


--
-- TOC entry 4055 (class 0 OID 0)
-- Dependencies: 235
-- Name: FUNCTION crosstab3(text); Type: ACL; Schema: public; Owner: rdsadmin
--

GRANT ALL ON FUNCTION public.crosstab3(text) TO "Fenland Data Warehouse Read Write";


--
-- TOC entry 4056 (class 0 OID 0)
-- Dependencies: 236
-- Name: FUNCTION crosstab4(text); Type: ACL; Schema: public; Owner: rdsadmin
--

GRANT ALL ON FUNCTION public.crosstab4(text) TO "Fenland Data Warehouse Read Write";


--
-- TOC entry 4057 (class 0 OID 0)
-- Dependencies: 237
-- Name: FUNCTION normal_rand(integer, double precision, double precision); Type: ACL; Schema: public; Owner: rdsadmin
--

GRANT ALL ON FUNCTION public.normal_rand(integer, double precision, double precision) TO "Fenland Data Warehouse Read Write";


--
-- TOC entry 4058 (class 0 OID 0)
-- Dependencies: 204
-- Name: TABLE measurementgroup; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON TABLE public.measurementgroup TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON TABLE public.measurementgroup TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4060 (class 0 OID 0)
-- Dependencies: 205
-- Name: SEQUENCE "MeasurementGroup_Id_seq"; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON SEQUENCE public."MeasurementGroup_Id_seq" TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON SEQUENCE public."MeasurementGroup_Id_seq" TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4061 (class 0 OID 0)
-- Dependencies: 206
-- Name: TABLE measurementtype; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON TABLE public.measurementtype TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON TABLE public.measurementtype TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4063 (class 0 OID 0)
-- Dependencies: 207
-- Name: SEQUENCE "MeasurementType_Id_seq"; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON SEQUENCE public."MeasurementType_Id_seq" TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON SEQUENCE public."MeasurementType_Id_seq" TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4064 (class 0 OID 0)
-- Dependencies: 208
-- Name: TABLE measurement; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON TABLE public.measurement TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON TABLE public.measurement TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4066 (class 0 OID 0)
-- Dependencies: 209
-- Name: SEQUENCE "Measurement_Id_seq"; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON SEQUENCE public."Measurement_Id_seq" TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON SEQUENCE public."Measurement_Id_seq" TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4067 (class 0 OID 0)
-- Dependencies: 210
-- Name: TABLE participant; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON TABLE public.participant TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON TABLE public.participant TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4069 (class 0 OID 0)
-- Dependencies: 211
-- Name: SEQUENCE "Participant_Id_seq"; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON SEQUENCE public."Participant_Id_seq" TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON SEQUENCE public."Participant_Id_seq" TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4070 (class 0 OID 0)
-- Dependencies: 212
-- Name: TABLE sourcetype; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON TABLE public.sourcetype TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON TABLE public.sourcetype TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4072 (class 0 OID 0)
-- Dependencies: 213
-- Name: SEQUENCE "SourceType_Id_seq"; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON SEQUENCE public."SourceType_Id_seq" TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON SEQUENCE public."SourceType_Id_seq" TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4073 (class 0 OID 0)
-- Dependencies: 214
-- Name: TABLE source; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON TABLE public.source TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON TABLE public.source TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4075 (class 0 OID 0)
-- Dependencies: 215
-- Name: SEQUENCE "Source_Id_seq"; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON SEQUENCE public."Source_Id_seq" TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON SEQUENCE public."Source_Id_seq" TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4076 (class 0 OID 0)
-- Dependencies: 216
-- Name: TABLE study; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON TABLE public.study TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON TABLE public.study TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4078 (class 0 OID 0)
-- Dependencies: 217
-- Name: SEQUENCE "Study_Id_seq"; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON SEQUENCE public."Study_Id_seq" TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON SEQUENCE public."Study_Id_seq" TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4079 (class 0 OID 0)
-- Dependencies: 226
-- Name: TABLE boundsdatetime; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.boundsdatetime TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON TABLE public.boundsdatetime TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4080 (class 0 OID 0)
-- Dependencies: 218
-- Name: TABLE boundsint; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON TABLE public.boundsint TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON TABLE public.boundsint TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4081 (class 0 OID 0)
-- Dependencies: 219
-- Name: TABLE boundsreal; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON TABLE public.boundsreal TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON TABLE public.boundsreal TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4082 (class 0 OID 0)
-- Dependencies: 220
-- Name: TABLE category; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON TABLE public.category TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON TABLE public.category TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4083 (class 0 OID 0)
-- Dependencies: 221
-- Name: TABLE datetimevalue; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON TABLE public.datetimevalue TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON TABLE public.datetimevalue TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4084 (class 0 OID 0)
-- Dependencies: 222
-- Name: TABLE measurementtypetogroup; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON TABLE public.measurementtypetogroup TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON TABLE public.measurementtypetogroup TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4085 (class 0 OID 0)
-- Dependencies: 223
-- Name: TABLE textvalue; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON TABLE public.textvalue TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON TABLE public.textvalue TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4086 (class 0 OID 0)
-- Dependencies: 224
-- Name: TABLE trial; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON TABLE public.trial TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON TABLE public.trial TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 4087 (class 0 OID 0)
-- Dependencies: 225
-- Name: TABLE units; Type: ACL; Schema: public; Owner: osm_dw
--

GRANT ALL ON TABLE public.units TO "Fenland Data Warehouse Read Write";
GRANT SELECT ON TABLE public.units TO "Fenland Data Warehouse Read Only";


--
-- TOC entry 1811 (class 826 OID 16752)
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: -; Owner: osm_dw
--

ALTER DEFAULT PRIVILEGES FOR ROLE osm_dw GRANT SELECT,INSERT,DELETE,UPDATE ON TABLES  TO "Fenland Data Warehouse Read Write";
ALTER DEFAULT PRIVILEGES FOR ROLE osm_dw GRANT SELECT ON TABLES  TO "Fenland Data Warehouse Read Only";


-- Completed on 2023-03-28 14:53:06

--
-- PostgreSQL database dump complete
--

