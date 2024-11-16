<?php

    namespace App\Modules\Tracking;

    use App\Models\Uom;
    use App\Modules\Tracking\Parsers\RocketShipItTrackingParser;
    use Illuminate\Support\Arr;
    use Illuminate\Support\Carbon;
    use Illuminate\Support\Facades\Cache;
    use Illuminate\Support\Facades\Log;

    /**
     * RocketShipItParser class processes and parses tracking data from RocketShipIt API responses.
     */
    class RocketShipItParser implements RocketShipItTrackingParser
    {
        /**
         * @var array Parsed data from the RocketShipIt API.
         */
        private array $data;

        /**
         * Constructor to initialize parser with tracking data.
         *
         * @param array $data The RocketShipIt tracking data.
         */
        public function __construct(array $data)
        {
            $this->data = $data;
        }

        /**
         * Retrieves all tracking events for a package.
         *
         * @return array List of tracking events.
         */
        public function getTrackingEvents(): array
        {
            $trackingEvents = [];

            if (!empty($this->data['packages']) && is_array($this->data['packages'])) {
                // Log multiple packages scenario for further inspection
                if (count($this->data['packages']) > 1) {
                    Log::channel('tracking')->info('Found tracking number with multiple packages.', [
                        'tracking_number' => $this->data['trackingNumber'],
                        'carrier_code' => $this->data['carrierCode'],
                    ]);
                    slack_message(
                        'rocket-shipit-multi-packages',
                        sprintf(
                            'RocketShipItParser.php: Found tracking number with multiple packages in a Rocket ShipIt response. Tracking Number: %s, Carrier Code: %s',
                            $this->data['trackingNumber'],
                            $this->data['carrierCode']
                        ),
                        'good'
                    );
                }

                // Extract tracking events from each package
                foreach ($this->data['packages'] as $package) {
                    if (!empty($package['activity']) && is_array($package['activity'])) {
                        $trackingEvents = array_merge($trackingEvents, $package['activity']);
                    }
                }
            }

            return $trackingEvents;
        }

        /**
         * Retrieves the unit of measurement for weight.
         *
         * @return mixed The unit of measurement (e.g., 'lbs', 'kg') or null if not found.
         */
        public function getWeightUom(): mixed
        {
            return $this->data['weight']['unit'] ?? null
                ? $this->getUomData($this->data['weight']['unit'])
                : null;
        }

        /**
         * Retrieves the unit of measurement for dimensions.
         *
         * @return mixed The unit of measurement (e.g., 'in', 'cm') or null if not found.
         */
        public function getDimensionUom(): mixed
        {
            return $this->data['dimensions']['unit'] ?? null
                ? $this->getUomData($this->data['dimensions']['unit'])
                : null;
        }

        /**
         * Retrieves or creates a unit of measurement (UOM) object.
         *
         * @param string $uomName The name of the unit of measurement.
         * @return mixed UOM data object or null.
         */
        public function getUomData(string $uomName): mixed
        {
            $cacheKey = 'uom.' . $uomName;

            // Check cache for UOM data
            $uomData = Cache::store('redis')->get($cacheKey);

            if ($uomData === null) {
                // Fetch or create UOM from the database
                $uom = Uom::firstOrCreate(['name' => $uomName]);
                $uomData = (object)['id' => $uom->id, 'name' => $uom->name];

                // Cache the UOM data for future use
                Cache::store('redis')->put($cacheKey, $uomData, config('cache.uom_ttl'));
            }

            return $uomData;
        }

        /**
         * Retrieves the weight of the package.
         *
         * @return mixed Weight value or null if not available.
         */
        public function getWeight(): mixed
        {
            return $this->data['weight']['amount'] ?? null;
        }

        /**
         * Retrieves the length of the package.
         *
         * @return mixed Length value or null if not available.
         */
        public function getLength(): mixed
        {
            return $this->data['dimensions']['length'] ?? null;
        }

        /**
         * Retrieves the width of the package.
         *
         * @return mixed Width value or null if not available.
         */
        public function getWidth(): mixed
        {
            return $this->data['dimensions']['width'] ?? null;
        }

        /**
         * Retrieves the height of the package.
         *
         * @return mixed Height value or null if not available.
         */
        public function getHeight(): mixed
        {
            return $this->data['dimensions']['height'] ?? null;
        }

        /**
         * Retrieves the origin address.
         *
         * @return array Origin address details.
         */
        public function getFromAddress(): array
        {
            return $this->data['origin'] ?? [];
        }

        /**
         * Retrieves the destination address.
         *
         * @return array Destination address details.
         */
        public function getToAddress(): array
        {
            return $this->data['destination'] ?? [];
        }

        /**
         * Retrieves the service code for the shipment.
         *
         * @return string Service code.
         */
        public function getServiceCode(): string
        {
            return $this->data['service']['code'] ?? '';
        }

        /**
         * Retrieves the service description for the shipment.
         *
         * @return string Service description.
         */
        public function getServiceDescription(): string
        {
            return $this->data['service']['description'] ?? '';
        }

        /**
         * Retrieves reference numbers associated with the shipment.
         *
         * @return mixed Reference numbers or null if not available.
         */
        public function getReferenceNumbers(): mixed
        {
            return $this->data['reference_numbers'] ?? null;
        }

        /**
         * Retrieves the estimated delivery time.
         *
         * @return mixed Formatted estimated delivery time or null if not available.
         */
        public function getEstimatedDelivery(): mixed
        {
            return isset($this->data['estimated_delivery'])
                ? date('Y-m-d H:i:s', strtotime($this->data['estimated_delivery']))
                : null;
        }

        /**
         * Retrieves the delivered time.
         *
         * @return mixed Formatted delivered time or null if not available.
         */
        public function getDeliveredTime(): mixed
        {
            return isset($this->data['delivered_time'])
                ? date('Y-m-d H:i:s', strtotime($this->data['delivered_time']))
                : null;
        }

        /**
         * Retrieves the pickup date.
         *
         * @return mixed Formatted pickup date or null if not available.
         */
        public function getPickupDate(): mixed
        {
            return isset($this->data['pickup_date'])
                ? date('Y-m-d H:i:s', strtotime($this->data['pickup_date']))
                : null;
        }

        /**
         * Retrieves the label creation date from the latest tracking activity.
         *
         * @return mixed Formatted label creation date or null if not available.
         */
        public function getLabelCreatedAt(): mixed
        {
            $latestTime = null;

            if (!empty($this->data['packages']) && is_array($this->data['packages'])) {
                foreach ($this->data['packages'] as $package) {
                    if (!empty($package['activity']) && is_array($package['activity'])) {
                        $latestEvent = end($package['activity']);
                        $latestTime = max($latestTime, $latestEvent['time']);
                    }
                }
            }

            return $latestTime ? Carbon::parse($latestTime)->format('Y-m-d H:i:s') : null;
        }

        /**
         * Retrieves the alternative tracking number if available.
         *
         * @return mixed Alternative tracking number or false if not available.
         */
        public function getAlternativeTrackingNumber(): mixed
        {
            return !empty($this->data['alternative_tracking_ids'])
                ? ($this->data['alternative_tracking_ids'][0]['value'] ?? false)
                : false;
        }
    }
