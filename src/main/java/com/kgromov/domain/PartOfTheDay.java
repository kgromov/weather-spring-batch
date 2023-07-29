package com.kgromov.domain;

import java.time.LocalTime;

public enum PartOfTheDay {
    MORNING() {
        @Override
        public LocalTime getStart() {
            return LocalTime.of(6, 0, 0);
        }

        @Override
        public LocalTime getEnd() {
            return LocalTime.NOON;
        }
    },
    AFTERNOON() {
        @Override
        public LocalTime getStart() {
            return LocalTime.NOON;
        }

        @Override
        public LocalTime getEnd() {
            return LocalTime.of(18, 0, 0);
        }
    },
    EVENING() {
        @Override
        public LocalTime getStart() {
            return LocalTime.of(18, 0, 0);
        }

        @Override
        public LocalTime getEnd() {
            return LocalTime.MAX;
        }
    },
    NIGHT() {
        @Override
        public LocalTime getStart() {
            return LocalTime.MIDNIGHT;
        }

        @Override
        public LocalTime getEnd() {
            return LocalTime.of(6, 0, 0);
        }
    };

    public abstract LocalTime getStart();

    public abstract LocalTime getEnd();
}
