<?php

    namespace App\Modules\Tracking;

    use App\Jobs\Tracking\ValidateTrackingNumberJob;
    use App\Models\LocationDetail;
    use App\Models\Timezone;
    use App\Models\TrackingDetail;
    use App\Models\TrackingEvent;
    use App\Models\TrackingNumber;
    use App\Models\TrackingNumberImportError;
    use App\Models\TrackingStatus;
    use App\Modules\Tracking\Parsers\RocketShipItTrackingParser;
    use App\Traits\CarrierServiceable;
    use App\Traits\NormalizeLocationDetailData;
    use App\Traits\PostalcodeTimezoneLookupTrait;
    use Carbon\Carbon;
    use Illuminate\Support\Facades\Cache;
    use Illuminate\Support\Facades\DB;
    use Illuminate\Support\Facades\Log;
    use Illuminate\Support\Facades\Redis;

    class RocketShipItTrackingProcessor
    {
        use PostalcodeTimezoneLookupTrait;
        use NormalizeLocationDetailData;
        use CarrierServiceable;

        protected RocketShipItClient $rocketShipitClient;

        /**
         * Constructor initializes the RocketShipIt client.
         *
         * @param RocketShipItClient $rocketShipitClient
         */
        public function __construct(RocketShipItClient $rocketShipitClient)
        {
            $this->rocketShipitClient = $rocketShipitClient;
        }

        /**
         * Processes a tracking number: fetches tracking data, parses, and stores results.
         *
         * @param object $carrier
         * @param TrackingNumber $trackingNumber
         * @return void
         */
        public function process(object $carrier, TrackingNumber $trackingNumber): void
        {
            try {
                // Fetch tracking data from API
                $trackingData = $this->fetchTrackingData($carrier, $trackingNumber);

                // Check for errors in tracking data
                if ($this->hasTrackingErrors($trackingData)) {
                    $this->handleTrackingErrors($trackingData, $trackingNumber);
                    return;
                }

                // Parse and store tracking data
                $parser = new RocketShipItTrackingParser($trackingData);
                $this->storeTrackingResultData($parser, $carrier, $trackingNumber);
            } catch (\Exception $e) {
                $this->logProcessingError($e, $carrier, $trackingNumber);
            }
        }

        /**
         * Retrieves only the "label created at" date from tracking data.
         *
         * @param object $carrier
         * @param TrackingNumber $trackingNumber
         * @return Carbon|null
         */
        public function processLabelCreatedAtOnly(object $carrier, TrackingNumber $trackingNumber): ?Carbon
        {
            try {
                $trackingData = $this->fetchTrackingData($carrier, $trackingNumber);

                if ($this->hasTrackingErrors($trackingData)) {
                    $this->logWarning('Label creation error', $trackingNumber, $trackingData['errors']);
                    return null;
                }

                $parser = new RocketShipItTrackingParser($trackingData);
                return $parser->getLabelCreatedAt();
            } catch (\Exception $e) {
                $this->logProcessingError($e, $carrier, $trackingNumber);
                return null;
            }
        }

        /**
         * Fetches tracking data using the RocketShipIt API.
         *
         * @param object $carrier
         * @param TrackingNumber $trackingNumber
         * @return array
         */
        private function fetchTrackingData(object $carrier, TrackingNumber $trackingNumber): array
        {
            $trackingData = $this->rocketShipitClient->getTrackingData($carrier, $trackingNumber->tracking_number)['data'];
            $trackingData['carrierCode'] = $carrier->code;
            $trackingData['trackingNumber'] = $trackingNumber->tracking_number;

            return $trackingData;
        }

        /**
         * Stores parsed tracking result data in the database.
         *
         * @param RocketShipItTrackingParser $parser
         * @param object $carrier
         * @param TrackingNumber $trackingNumber
         * @return void
         */
        public function storeTrackingResultData(RocketShipItTrackingParser $parser, object $carrier, TrackingNumber $trackingNumber): void
        {
            DB::transaction(function () use ($parser, $carrier, $trackingNumber) {
                // Retrieve carrier service details
                $carrierServiceData = $this->getCarrierServiceData(
                    $parser->getServiceDescription(),
                    $parser->getServiceCode(),
                    $carrier->id
                );

                // Store tracking events and get the latest event
                $latestEvent = $this->storeTrackingEvents($parser, $trackingNumber, $carrier);

                // Prepare shipment details
                $shipmentDetails = [
                    'tracking_number_id'       => $trackingNumber->id,
                    'carrier_service_name_id'  => $carrierServiceData['carrier_service_name_id'],
                    'carrier_service_code_id'  => $carrierServiceData['carrier_service_code_id'],
                    'carrier_id'               => $carrier->id,
                    'ship_from_address'        => $parser->getFromAddress(),
                    'ship_to_address'          => $parser->getToAddress(),
                    'reference_numbers'        => $parser->getReferenceNumbers(),
                    'estimated_delivery'       => $parser->getEstimatedDelivery(),
                    'pickup_date'              => $parser->getPickupDate(),
                    'shipment_weight'          => $parser->getWeight(),
                    'shipment_length'          => $parser->getLength(),
                    'shipment_width'           => $parser->getWidth(),
                    'shipment_height'          => $parser->getHeight(),
                ];

                // Save shipment details
                $this->storeShipmentDetails($shipmentDetails);

                // Update tracking number with the latest event and label creation date
                $trackingNumber->update([
                    'latest_event_id'  => $latestEvent?->id,
                    'queue_status'     => $latestEvent?->trackingStatus->terminal_status ?? 0,
                    'label_created_at' => $parser->getLabelCreatedAt(),
                ]);

                // Handle alternate tracking numbers
                if ($altTrackingNumber = $parser->getAlternativeTrackingNumber()) {
                    $this->handleAltTrackingNumber($altTrackingNumber, $trackingNumber);
                }
            });
        }

        /**
         * Handles alternate tracking numbers by dispatching validation jobs.
         *
         * @param string $altTrackingNumber
         * @param TrackingNumber $trackingNumber
         * @return void
         */
        public function handleAltTrackingNumber(string $altTrackingNumber, TrackingNumber $trackingNumber): void
        {
            foreach ($trackingNumber->teams as $team) {
                ValidateTrackingNumberJob::dispatch(
                    trackingNumber: $altTrackingNumber,
                    carrier: config('carriers')->USPS,
                    teamId: $team->id,
                    parentTrackingNumberId: $trackingNumber->id
                )->onQueue('validate_tracking_numbers-' . app()->environment());
            }
        }

        /**
         * Stores tracking events and returns the latest event.
         *
         * @param RocketShipItTrackingParser $parser
         * @param TrackingNumber $trackingNumber
         * @param object $carrier
         * @return TrackingEvent|null
         */
        public function storeTrackingEvents(RocketShipItTrackingParser $parser, TrackingNumber $trackingNumber, object $carrier): ?TrackingEvent
        {
            $events = $parser->getTrackingEvents();

            if (empty($events)) {
                $this->logWarning('No tracking events found', $trackingNumber);
                return null;
            }

            return collect($events)->map(fn($event) => $this->storeTrackingEvent($event, $trackingNumber, $carrier))->first();
        }

        /**
         * Stores a single tracking event.
         *
         * @param array $event
         * @param TrackingNumber $trackingNumber
         * @param object $carrier
         * @return TrackingEvent
         */
        public function storeTrackingEvent(array $event, TrackingNumber $trackingNumber, object $carrier): TrackingEvent
        {
            $trackingStatus = $this->firstOrCreateTrackingStatus(
                $event['status_code'],
                $event['status_description'],
                $event['status_type'],
                $carrier->code
            );

            $locationDetail = $this->firstOrCreateLocationDetail($event['location'] ?? []);

            return TrackingEvent::updateOrCreate(
                [
                    'tracking_number_id' => $trackingNumber->id,
                    'tracking_status_id' => $trackingStatus->id,
                    'location_detail_id' => $locationDetail?->id,
                ],
                [
                    'local_datetime'       => $event['time'],
                    'location_description' => $event['location']['description'] ?? null,
                ]
            );
        }

        /**
         * Fetch or create carrier service data (name and code IDs).
         *
         * @param string $serviceDescription
         * @param string $serviceCode
         * @param int $carrierId
         * @return array
         */
        private function getCarrierServiceData(string $serviceDescription, string $serviceCode, int $carrierId): array
        {
            $nameCacheKey = "carrier_service_name_{$serviceDescription}_{$carrierId}";
            $codeCacheKey = "carrier_service_code_{$serviceCode}_{$carrierId}";

            $carrierServiceNameId = Cache::store('redis')->rememberForever($nameCacheKey, function () use ($serviceDescription, $carrierId) {
                return DB::table('carrier_service_names')->insertOrIgnore([
                    'description' => $serviceDescription,
                    'carrier_id'  => $carrierId,
                    'created_at'  => now(),
                    'updated_at'  => now(),
                ]) ? DB::table('carrier_service_names')->where('description', $serviceDescription)->value('id') : null;
            });

            $carrierServiceCodeId = Cache::store('redis')->rememberForever($codeCacheKey, function () use ($serviceCode, $carrierId) {
                return DB::table('carrier_service_codes')->insertOrIgnore([
                    'code'       => $serviceCode,
                    'carrier_id' => $carrierId,
                    'created_at' => now(),
                    'updated_at' => now(),
                ]) ? DB::table('carrier_service_codes')->where('code', $serviceCode)->value('id') : null;
            });

            return [
                'carrier_service_name_id' => $carrierServiceNameId,
                'carrier_service_code_id' => $carrierServiceCodeId,
            ];
        }

        /**
         * Stores shipment details in the database.
         *
         * @param array $details
         * @return TrackingDetail
         */
        public function storeShipmentDetails(array $details): TrackingDetail
        {
            return TrackingDetail::updateOrCreate(
                ['tracking_number_id' => $details['tracking_number_id']],
                $details
            );
        }

        /**
         * Fetch or create a location detail record.
         *
         * @param array $location
         * @return LocationDetail
         */
        private function firstOrCreateLocationDetail(array $location): LocationDetail
        {
            $normalized = $this->normalizeLocationData($location);
            $hash = md5(json_encode($normalized));

            return Cache::store('redis')->rememberForever("location_detail_{$hash}", function () use ($normalized) {
                return LocationDetail::firstOrCreate($normalized);
            });
        }

        /**
         * Fetch or create a tracking status record.
         *
         * @param string $code
         * @param string $description
         * @param string $type
         * @param string $carrier
         * @return TrackingStatus
         */
        private function firstOrCreateTrackingStatus(string $code, string $description, string $type, string $carrier): TrackingStatus
        {
            return Cache::store('redis')->rememberForever("tracking_status_{$code}", function () use ($code, $description, $type, $carrier) {
                return TrackingStatus::firstOrCreate(
                    ['code' => $code],
                    [
                        'description'     => $description,
                        'type'            => $type,
                        'terminal_status' => in_array(strtolower($description), config("global.terminal_status.{$carrier}")) ? 1 : 0,
                    ]
                );
            });
        }

        /**
         * Handles tracking errors by logging and updating tracking status.
         *
         * @param array $trackingData
         * @param TrackingNumber $trackingNumber
         * @return void
         */
        private function handleTrackingErrors(array $trackingData, TrackingNumber $trackingNumber): void
        {
            $ignoredErrorCodes = ['-2147219283', '-2147219284'];

            $hasRelevantErrors = collect($trackingData['errors'] ?? [])
                ->contains(fn($error) => in_array($error['code'], $ignoredErrorCodes));

            if (!$hasRelevantErrors) {
                $this->logWarning('Tracking error encountered', $trackingNumber, $trackingData['errors']);
            }

            // Update queue status to indicate failure
            $trackingNumber->update(['queue_status' => 0]);
        }

        /**
         * Logs warnings with context information.
         *
         * @param string $message
         * @param TrackingNumber $trackingNumber
         * @param array|null $errors
         * @return void
         */
        private function logWarning(string $message, TrackingNumber $trackingNumber, array $errors = null): void
        {
            $logData = ['Tracking Number' => $trackingNumber->toArray()];
            if ($errors) {
                $logData['Errors'] = $errors;
            }

            Log::channel('tracking')->warning($message, $logData);
        }

        /**
         * Logs errors during processing and creates an import error entry.
         *
         * @param \Exception $e
         * @param object $carrier
         * @param TrackingNumber $trackingNumber
         * @return void
         */
        private function logProcessingError(\Exception $e, object $carrier, TrackingNumber $trackingNumber): void
        {
            Log::channel('tracking')->error('Error processing tracking number', [
                'error_message'   => $e->getMessage(),
                'tracking_number' => $trackingNumber->tracking_number,
            ]);

            TrackingNumberImportError::create([
                'carrier_id'      => $carrier->id,
                'error_message'   => $e->getMessage(),
                'tracking_number' => $trackingNumber->tracking_number,
            ]);
        }

        /**
         * Checks for errors in tracking data.
         *
         * @param array $data
         * @return bool
         */
        public function hasTrackingErrors(array $data): bool
        {
            $errorCodesToCheck = ['-2147219283', '-2147219284'];
            return !empty(array_filter($data['errors'] ?? [], fn($error) => in_array($error['code'], $errorCodesToCheck)));
        }
    }
