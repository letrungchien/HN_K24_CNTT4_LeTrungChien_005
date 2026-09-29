package org.example.rentalservice.models.services.impl;

import feign.FeignException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import org.example.rentalservice.clients.VehicleClient;
import org.example.rentalservice.exceptions.VehicleNotFoundException;
import org.example.rentalservice.exceptions.VehicleServiceException;
import org.example.rentalservice.models.dto.responses.VehicleResponse;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class VehicleGatewayService {

    private final VehicleClient vehicleClient;

    @CircuitBreaker(name = "vehicleService", fallbackMethod = "getVehicleByIdFallback")
    public VehicleResponse getVehicleById(Long vehicleId) {
        try {
            return vehicleClient.getVehicleById(vehicleId);
        } catch (FeignException.NotFound exception) {
            throw new VehicleNotFoundException(vehicleId);
        } catch (FeignException exception) {
            throw new VehicleServiceException("Vehicle service is unavailable", exception);
        }
    }

    private VehicleResponse getVehicleByIdFallback(Long vehicleId, Throwable throwable) {
        if (throwable instanceof VehicleNotFoundException notFoundException) {
            throw notFoundException;
        }
        if (throwable instanceof VehicleServiceException serviceException) {
            throw serviceException;
        }
        throw new VehicleServiceException("Vehicle service is unavailable", throwable);
    }
}
