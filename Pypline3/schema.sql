drop table if exists visit;
CREATE TABLE visit(
    visit_id text NOT NULL,
    experiment_directory text NOT NULL, 
    year integer NOT NULL,
    PRIMARY KEY (visit_id)
); 

drop table if exists nxs;
CREATE TABLE nxs(
    nxs_file text NOT NULL,
    scan_command text NOT NULL,
    descriptive_title text NOT NULL,
    scan_type text NOT NULL,
    outfile_prefix text NOT NULL,
    file_code integer NOT NULL,
    dat_dir text NOT NULL,
    number_of_exposures integer NOT NULL,
    exposure_time real NOT NULL,
    file_time text NOT NULL,
    visit_id text NOT NULL,
    PRIMARY KEY (nxs_file),
    FOREIGN KEY (visit_id) REFERENCES visit(visit_id)
);

drop table if exists raw_dat;
CREATE TABLE raw_dat(
    rawdat_file text NOT NULL,
    outdat_file text NOT NULL,
    output_index text NOT NULL,
    nxs_file text NOT NULL,
    PRIMARY KEY (rawdat_file),
    FOREIGN KEY (nxs_file) REFERENCES nxs(nxs_file)
);

drop table if exists averaging_instance;
CREATE TABLE averaging_instance(
    averaging_instance integer PRIMARY KEY AUTOINCREMENT,
    is_good integer NOT NULL,
    rawdat_file text NOT NULL,
    avdat_file text NOT NULL,
    FOREIGN KEY (rawdat_file) REFERENCES raw_dat(rawdat_file),
    FOREIGN KEY (avdat_file) REFERENCES av_dat(avdat_file)
);

drop table if exists av_dat;
CREATE TABLE av_dat(
    avdat_file text NOT NULL,
    number_input_files integer NOT NULL,
    accepted_input_files real NOT NULL,
    PRIMARY KEY (avdat_file)
);


drop table if exists sub_dat;
CREATE TABLE sub_dat(
    subdat_file text NOT NULL,
    sample_file text NOT NULL,
    buffer_file text NOT NULL,
    highq_signal real,
    rg real,
    i_zero real,
    volume real,
    mass real ,
    start_guinier integer,
    end_guinier integer,
    PRIMARY KEY (subdat_file),
    FOREIGN KEY (sample_file) REFERENCES av_dat(avdat_file),
    FOREIGN KEY (buffer_file) REFERENCES av_dat(avdat_file)
);