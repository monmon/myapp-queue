CREATE TABLE `myqueue` (
    `id`           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `locked_until` TIMESTAMP NOT NULL DEFAULT "0000-00-00 00:00:00",
    `worker`       VARCHAR(255) NOT NULL,
    `data`         BLOB NOT NULL,
    PRIMARY KEY  (`id`)
) ENGINE=InnoDB;
