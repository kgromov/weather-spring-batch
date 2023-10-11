package com.kgromov.service;

import com.kgromov.domain.City;
import com.kgromov.dtos.TemperatureMeasurementsDto;
import com.kgromov.dtos.WeatherMeasurementDto;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.data.util.Pair;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

// TODO: extract common part to  default interface method;
// sync data from Meteopost [2010-01-26; 2022-03-21]; sinoptik - [2022-03-21; now)
@Slf4j
@Service
public class MeteopostExtractor implements TemperatureExtractor {
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("H:mm");

    // last measurements for 2022-03-21 :(
    @Override
    public Optional<TemperatureMeasurementsDto> getTemperatureAt(City city, LocalDate measurementDate) {
        long start = System.nanoTime();
        log.info("Collecting daily temperature for city {}, at {}", city, measurementDate);
        // TODO: add preconfigured RestTemplateBuilder as bean
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", MediaType.APPLICATION_FORM_URLENCODED.toString());

        // TODO: parse from LocalDate; extend City or make interface with codes
        var requestBody = new LinkedMultiValueMap<>();
        requestBody.add("d", String.valueOf(measurementDate.getDayOfMonth()));
        requestBody.add("m", String.format("%02d", measurementDate.getMonthValue()));
        requestBody.add("y", String.valueOf(measurementDate.getYear()));
        requestBody.add("city", "UKOO");
        requestBody.add("arc", "1");

        var formEntity = new HttpEntity<>(requestBody, headers);
        ResponseEntity<String> response = restTemplate.exchange("https://meteopost.com/weather/archive/",
                HttpMethod.POST, formEntity, String.class
        );
        Document document = Jsoup.parse(response.getBody());
        Element weatherTable = document.getElementById("arc");
        Elements timeCells = weatherTable.select("tbody td:nth-child(1)");
        Elements temperatureCells = weatherTable.select("tbody td:nth-child(2)");
        List<WeatherMeasurementDto> dailyMeasurements = IntStream.range(0, timeCells.size()).boxed()
                .map(index -> Pair.of(timeCells.get(index), temperatureCells.get(index)))
                .map(data -> mapToWeatherMeasurementDto(data.getFirst(), data.getSecond()))
                .collect(Collectors.toList());

        TemperatureMeasurementsDto temperatureMeasurementsDto = new TemperatureMeasurementsDto();
        temperatureMeasurementsDto.setDate(measurementDate);
        temperatureMeasurementsDto.setDailyMeasurements(dailyMeasurements);
        log.info("Time to extract cell values = {} ms", TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS));
        return Optional.of(temperatureMeasurementsDto);
    }

    private static WeatherMeasurementDto mapToWeatherMeasurementDto(Element timeElement, Element tempElement) {
        String time = timeElement.text().trim();
        String temperature = tempElement.text().trim();
        LocalTime parsedTime = LocalTime.parse(time, TIME_FORMATTER);
        int parsedTemp = Integer.parseInt(temperature.substring(0, temperature.length() - 1));
        return new WeatherMeasurementDto(parsedTime, parsedTemp);
    }

    public static void main(String[] args) {
        Optional<TemperatureMeasurementsDto> temperatureAt = new MeteopostExtractor().getTemperatureAt(City.ODESSA, LocalDate.now().minusYears(2));
        temperatureAt.ifPresent(temp -> log.info("Temp for today = {}", temp));
    }
}
