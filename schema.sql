CREATE TABLE IF NOT EXISTS word (
    id       INT NOT NULL AUTO_INCREMENT,
    word     TEXT NOT NULL UNIQUE,
    parent   INT,
    priority INT NOT NULL,
    status   ENUM (
        "WAITING_FOR_GENERATION", "GENERATING", "GENERATION_FAILED",  "GENERATION_COMPLETE",
        "WAITING_FOR_CLASSIFICATION", "CLASSIFYING", "CLASSIFICATION_FAILED", "CLASSIFIED_AS_INVALID",
        "WAITING_FOR_SEARCH", "SEARCHING", "SEARCH_FAILED", "SEARCH_COMPLETE"
    ) NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (parent) REFERENCES word(id)
);

CREATE TABLE IF NOT EXISTS link_blacklist (
    id       INT NOT NULL AUTO_INCREMENT,
    word     TEXT NOT NULL UNIQUE,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS page (
    id              INT NOT NULL AUTO_INCREMENT,
    link            TEXT NOT NULL,
    domain          TEXT NOT NULL,
    word_source     INT,
    follower_source INT,
    content         MEDIUMTEXT,
    content_size    INT,
    schema          MEDIUMTEXT,
    priority        INT NOT NULL,
    status          ENUM (
        "WAITING_FOR_DOWNLOAD", "DOWNLOADING", "DOWNLOAD_FAILED", 
        "WAITING_FOR_EXTRACTION", "EXTRACTING", "EXTRACTION_FAILED", 
        "WAITING_FOR_PARSING", "PARSING", "PARSING_INCOMPLETE_RECIPE",
        "WAITING_FOR_FOLLOWING", "FOLLOWING", "FOLLOWING_COMPLETE", "FOLLOWING_FAILED"
    ) NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS recipe (
    id                 INT NOT NULL AUTO_INCREMENT,
    page               INT NOT NULL,
    title              TEXT,
    description        TEXT,
    date               DATE,
    rating             FLOAT,
    rating_count       INT,
    prep_time_seconds  INT,
    cook_time_seconds  INT,
    total_time_seconds INT,
    servings           TEXT,
    calories           FLOAT,
    carbohydrates      FLOAT,
    cholesterol        FLOAT,
    fat                FLOAT,
    fiber              FLOAT,
    protein            FLOAT,
    saturated_fat      FLOAT,
    sodium             FLOAT,
    sugar              FLOAT,
    PRIMARY KEY (id),
    CONSTRAINT UniqueRecipe UNIQUE (title, description),
    FOREIGN KEY (page) REFERENCES page(id)
);

CREATE TABLE IF NOT EXISTS keyword (
    id      INT NOT NULL AUTO_INCREMENT,
    keyword TEXT NOT NULL UNIQUE,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS recipe_keyword (
    recipe  INT NOT NULL,
    keyword INT NOT NULL,
    PRIMARY KEY (recipe, keyword),
    FOREIGN KEY (recipe) REFERENCES recipe(id),
    FOREIGN KEY (keyword) REFERENCES keyword(id)
);

CREATE TABLE IF NOT EXISTS author (
    id   INT NOT NULL AUTO_INCREMENT,
    name TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS recipe_author (
    recipe INT NOT NULL,
    author INT NOT NULL,
    PRIMARY KEY (recipe, author),
    FOREIGN KEY (recipe) REFERENCES recipe(id),
    FOREIGN KEY (author) REFERENCES author(id)
);

CREATE TABLE IF NOT EXISTS image (
    id    INT NOT NULL AUTO_INCREMENT,
    image TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS recipe_image (
    recipe INT NOT NULL,
    image  INT NOT NULL,
    PRIMARY KEY (recipe, image),
    FOREIGN KEY (recipe) REFERENCES recipe(id),
    FOREIGN KEY (image) REFERENCES image(id)
);

CREATE TABLE IF NOT EXISTS ingredient (
    id         INT NOT NULL AUTO_INCREMENT,
    ingredient TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS recipe_ingredient (
    recipe     INT NOT NULL,
    ingredient INT NOT NULL,
    PRIMARY KEY (recipe, ingredient),
    FOREIGN KEY (recipe) REFERENCES recipe(id),
    FOREIGN KEY (ingredient) REFERENCES ingredient(id)
);

CREATE TABLE IF NOT EXISTS instruction (
    id          INT NOT NULL AUTO_INCREMENT,
    instruction TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS recipe_instruction (
    recipe      INT NOT NULL,
    instruction INT NOT NULL,
    PRIMARY KEY (recipe, instruction),
    FOREIGN KEY (recipe) REFERENCES recipe(id),
    FOREIGN KEY (instruction) REFERENCES instruction(id)
);

CREATE TABLE IF NOT EXISTS word_statistic (
    timestamp                  DateTime NOT NULL,
    waiting_for_generation     INT NOT NULL,
    generating                 INT NOT NULL,
    generation_failed          INT NOT NULL,
    generation_complete        INT NOT NULL,
    waiting_for_classification INT NOT NULL,
    classifying                INT NOT NULL,
    classification_failed      INT NOT NULL,
    classified_as_invalid      INT NOT NULL,
    waiting_for_search         INT NOT NULL,
    searching                  INT NOT NULL,
    search_failed              INT NOT NULL,
    search_complete            INT NOT NULL
);

CREATE TABLE IF NOT EXISTS page_statistic (
    timestamp                 DateTime NOT NULL,
    waiting_for_download      INT NOT NULL,
    downloading               INT NOT NULL,
    download_failed           INT NOT NULL,
    waiting_for_extraction    INT NOT NULL,
    extracting                INT NOT NULL,
    extraction_failed         INT NOT NULL,
    waiting_for_parsing       INT NOT NULL,
    parsing                   INT NOT NULL,
    parsing_incomplete_recipe INT NOT NULL,
    waiting_for_following     INT NOT NULL,
    following                 INT NOT NULL,
    following_complete        INT NOT NULL,
    following_failed          INT NOT NULL,
    total_content_size        INT NOT NULL
);

CREATE TABLE IF NOT EXISTS recipe_component_statistic (
    timestamp         DateTime NOT NULL,
    recipe_count      INT NOT NULL,
    keyword_count     INT NOT NULL,
    author_count      INT NOT NULL,
    image_count       INT NOT NULL,
    ingredient_count  INT NOT NULL,
    instruction_count INT NOT NULL
);

CREATE TABLE IF NOT EXISTS recipe_statistic (
    timestamp               DateTime NOT NULL,
    with_keywords           INT NOT NULL,
    with_authors            INT NOT NULL,
    with_images             INT NOT NULL,
    with_ingredients        INT NOT NULL,
    with_instructions       INT NOT NULL,
    with_title              INT NOT NULL,
    with_description        INT NOT NULL,
    with_date               INT NOT NULL,
    with_rating             INT NOT NULL,
    with_rating_count       INT NOT NULL,
    with_prep_time_seconds  INT NOT NULL,
    with_cook_time_seconds  INT NOT NULL,
    with_total_time_seconds INT NOT NULL,
    with_servings           INT NOT NULL,
    with_calories           INT NOT NULL,
    with_carbohydrates      INT NOT NULL,
    with_cholesterol        INT NOT NULL,
    with_fat                INT NOT NULL,
    with_fiber              INT NOT NULL,
    with_protein            INT NOT NULL,
    with_saturated_fat      INT NOT NULL,
    with_sodium             INT NOT NULL,
    with_sugar              INT NOT NULL
);

