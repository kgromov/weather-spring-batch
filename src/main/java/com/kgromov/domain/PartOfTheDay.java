package com.kgromov.domain;

import java.time.LocalTime;

public enum PartOfTheDay {
    MORNING() {
        @Override
        LocalTime getStart() {
            return LocalTime.of(6, 0, 0);
        }

        @Override
        LocalTime getEnd() {
            return LocalTime.NOON;
        }
    },
    AFTERNOON() {
        @Override
        LocalTime getStart() {
            return LocalTime.NOON;
        }

        @Override
        LocalTime getEnd() {
            return LocalTime.of(18, 0, 0);
        }
    },
    EVENING() {
        @Override
        LocalTime getStart() {
            return LocalTime.of(18, 0, 0);
        }

        @Override
        LocalTime getEnd() {
            return LocalTime.MAX;
        }
    },
    NIGHT() {
        @Override
        LocalTime getStart() {
            return LocalTime.MIDNIGHT;
        }

        @Override
        LocalTime getEnd() {
            return LocalTime.of(6, 0, 0);
        }
    };

    abstract LocalTime getStart();

    abstract LocalTime getEnd();
}
