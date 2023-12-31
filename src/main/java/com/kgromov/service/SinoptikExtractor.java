package com.kgromov.service;

import com.kgromov.config.WeatherSource;
import com.kgromov.domain.City;
import com.kgromov.dtos.TemperatureMeasurementsDto;
import com.kgromov.dtos.WeatherMeasurementDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@RequiredArgsConstructor
@Service
public class SinoptikExtractor implements TemperatureExtractor {
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("H :mm");
    private final WeatherSource weatherSource;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public Optional<TemperatureMeasurementsDto> getTemperatureAt(City city, LocalDate measurementDate) {
        long start = System.nanoTime();
        log.info("Collecting daily temperature for city {}, at {}", city, measurementDate);
        String dateFormatted = DATE_FORMATTER.format(measurementDate);
        try {
            String encodedCityName = URLEncoder.encode(city.getKeyWord(), StandardCharsets.UTF_8);
            String url = weatherSource.sinoptikUrl() + '/' + encodedCityName + '/' + dateFormatted;
            Connection connection = Jsoup.connect(url);
            Document document = connection.get();

            Element weatherTable = document.selectFirst("table.weatherDetails");
            Elements timeCells = weatherTable.select("tbody>tr.gray.time>td");
            Elements temperatureCells = weatherTable.select("tbody>tr.temperature>td");

            List<WeatherMeasurementDto> dailyMeasurements = IntStream.range(0, timeCells.size()).boxed()
                    .map(index -> Pair.of(timeCells.get(index), temperatureCells.get(index)))
                    .map(data -> mapToWeatherMeasurementDto(data.getFirst(), data.getSecond()))
                    .collect(Collectors.toList());

            TemperatureMeasurementsDto temperatureMeasurementsDto = new TemperatureMeasurementsDto();
            temperatureMeasurementsDto.setDate(measurementDate);
            temperatureMeasurementsDto.setDailyMeasurements(dailyMeasurements);
            return Optional.of(temperatureMeasurementsDto);
        } catch (NullPointerException e) {
            log.error("No weather for specified date {} in city = {}", dateFormatted, city);
            return Optional.empty();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            log.info("Time to extract cell values = {} ms", TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS));
        }
    }

    private static WeatherMeasurementDto mapToWeatherMeasurementDto(Element timeElement,Element tempElement){
        String time = timeElement.text().trim();
        String temperature = tempElement.text().trim();
        LocalTime parsedTime = LocalTime.parse(time, TIME_FORMATTER);
        int parsedTemp = Integer.parseInt(temperature.substring(0, temperature.length() - 1));
        return new WeatherMeasurementDto(parsedTime, parsedTemp);
    }
}
