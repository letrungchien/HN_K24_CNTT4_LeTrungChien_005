package org.example.rentalservice.models.services.impl;

import lombok.RequiredArgsConstructor;
import org.example.rentalservice.clients.VehicleClient;
import org.example.rentalservice.models.constants.RentalStatus;
import org.example.rentalservice.models.dto.requests.CreateRentalDetailRequest;
import org.example.rentalservice.models.dto.requests.CreateRentalRequest;
import org.example.rentalservice.models.dto.responses.RentalDetailResponse;
import org.example.rentalservice.models.dto.responses.RentalResponse;
import org.example.rentalservice.models.dto.responses.VehicleResponse;
import org.example.rentalservice.models.entities.Rental;
import org.example.rentalservice.models.entities.RentalDetail;
import org.example.rentalservice.models.repositories.RentalDetailRepository;
import org.example.rentalservice.models.repositories.RentalRepository;
import org.example.rentalservice.models.services.RentalService;
import org.jspecify.annotations.NonNull;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class RentalServiceImpl implements RentalService {

        private final RentalRepository rentalRepository;
        private final RentalDetailRepository rentalDetailRepository;
        private final VehicleGatewayService vehicleGatewayService;
        private final KafkaTemplate<String, String> kafkaTemplate;
        private final VehicleClient vehicleClient;
        private static final String RENTAL_CREATED_TOPIC = "rental-created";

        @Override
        @Transactional
        public RentalResponse createRental(CreateRentalRequest request) {


                
                Rental rental = Rental.builder()
                                .customerName(request.customerName())
                                .customerEmail(request.customerEmail())
                                .totalAmount(0D)
                                .status(RentalStatus.PENDING)
                                .build();
                rental = rentalRepository.save(rental);

                List<RentalDetailResponse> itemResponses = new ArrayList<>();
//              double totalAmount =vehicleGatewayService.getVehicleById().pricePerDay()
                double totalAmount = 0D;

                for (CreateRentalDetailRequest item : request.items()) {
                        VehicleResponse vehicle = vehicleGatewayService.getVehicleById(item.vehicleId());

                        double lineTotal = vehicle.pricePerDay() * item.rentalDays();

                        RentalDetail detail = RentalDetail.builder()
                                        .rental(rental)
                                        .vehicleId(vehicle.vehicleId())
                                        .rentalDays(item.rentalDays())
                                        .pricePerDay(vehicle.pricePerDay())
                                        .build();
                        detail = rentalDetailRepository.save(detail);

                        itemResponses.add(new RentalDetailResponse(
                                        detail.getId(), vehicle.vehicleId(), vehicle.vehicleName(), vehicle.vehicleType(),
                                        item.rentalDays(), vehicle.pricePerDay(), lineTotal));
                        totalAmount += lineTotal;
                }



                rental.setTotalAmount(totalAmount);
                rental = rentalRepository.save(rental);


                   kafkaTemplate.send(RENTAL_CREATED_TOPIC, request.customerEmail());


               return new RentalResponse(
                               rental.getId(),
                       rental.getCustomerName(),
                       rental.getCustomerEmail(),
                                rental.getTotalAmount(),
                       rental.getStatus(),
                       itemResponses);
        }
}
