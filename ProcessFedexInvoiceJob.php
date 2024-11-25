<?php

    namespace App\Jobs\InvoiceImporting\Fedex;

    use App\Models\FedexInvoice;
    use App\Models\FedexInvoiceCharge;
    use App\Models\FedexSurchargeName;
    use App\Traits\CarrierServiceable;
    use App\Traits\JobRetryable;
    use Carbon\Carbon;
    use Illuminate\Bus\Queueable;
    use Illuminate\Contracts\Queue\ShouldQueue;
    use Illuminate\Foundation\Bus\Dispatchable;
    use Illuminate\Queue\InteractsWithQueue;
    use Illuminate\Queue\SerializesModels;
    use Illuminate\Support\Facades\Cache;
    use Illuminate\Support\Facades\Log;

    /**
     * Job to process a single FedEx invoice.
     * Handles tracking charges, surcharge creation, and invoice persistence.
     */
    class ProcessFedexInvoiceJob implements ShouldQueue
    {
        use Dispatchable, InteractsWithQueue, Queueable, SerializesModels, JobRetryable, CarrierServiceable;

        // Invoice data and related properties
        public $invoice;
        public $fileId;
        public $trackingChargeData = [];
        public $trackingIDChargeDescription;
        public $trackingIDChargeAmount;

        // Cache key for storing existing invoices
        private string $cacheKey = 'existing-fedex-invoices';

        /**
         * Create a new job instance.
         *
         * @param array $invoice Invoice data to be processed.
         * @param int $fileId ID of the related invoice file.
         */
        public function __construct(array $invoice, int $fileId)
        {
            $this->setDefaultRetryConfig();
            $this->invoice = $invoice;
            $this->fileId = $fileId;
        }

        /**
         * Execute the job.
         * Processes FedEx invoice data, handles duplicates, and persists tracking charges.
         */
        public function handle(): void
        {
            try {
                // Check if the invoice already exists in the database
                $existingInvoice = FedexInvoice::with('charges:id,fedex_invoice_id,description,amount')
                    ->where('express_ground_tracking_id', $this->invoice['express_ground_tracking_id'])
                    ->first();

                if ($existingInvoice) {
                    // Process tracking charges for existing invoice
                    $trackingCharges = $existingInvoice->charges()->pluck('description')->toArray();
                    $this->setTrackingChargeData($trackingCharges, $existingInvoice->id);
                    $this->insertTrackingCharges();
                    return; // Skip creating a new invoice entry
                }

                // Process new invoice data
                $this->prepareInvoiceData();
                $fedexInvoice = $this->createFedexInvoice();
                $this->setTrackingChargeData([], $fedexInvoice->id);
                $this->insertTrackingCharges();

            } catch (\Exception $e) {
                $this->failed($e, [
                    'message' => 'Error processing FedEx Invoice',
                    'invoice' => $this->invoice,
                ]);
            }
        }

        /**
         * Prepares the invoice data by formatting and removing unnecessary fields.
         */
        private function prepareInvoiceData(): void
        {
            $this->trackingIDChargeDescription = $this->invoice['Tracking_ID_Charge_Description'] ?? [];
            $this->trackingIDChargeAmount = $this->invoice['Tracking_ID_Charge_Amount'] ?? [];

            // Remove fields not required in the FedExInvoice database entry
            unset(
                $this->invoice['Tracking_ID_Charge_Description'],
                $this->invoice['Tracking_ID_Charge_Amount'],
                $this->invoice['commodity_description'],
                $this->invoice['commodity_country_territory_code']
            );

            // Set additional properties for the invoice
            $this->invoice['invoice_file_id'] = $this->fileId;
            $this->invoice['invoice_date'] = Carbon::parse($this->invoice['invoice_date']);
            $this->invoice['carrier_service_name_id'] = $this->getCarrierServiceNameId(
                $this->invoice['service_type'],
                config('carriers')->FEDEX->id,
                'Fedex Invoice'
            );

            // Remove service type after retrieving carrier service name ID
            unset($this->invoice['service_type']);

            // Format monetary values by removing commas
            $this->invoice = array_map(fn($val) => is_array($val) ? $val : str_replace(',', '', $val), $this->invoice);
        }

        /**
         * Creates a new FedEx invoice record in the database.
         *
         * @return FedexInvoice The created invoice model instance.
         */
        private function createFedexInvoice(): FedexInvoice
        {
            return FedexInvoice::create(array_filter($this->invoice));
        }

        /**
         * Sets tracking charge data for an invoice.
         *
         * @param array $charges Existing charges for the invoice.
         * @param int $invoiceId ID of the FedEx invoice.
         */
        public function setTrackingChargeData(array $charges, int $invoiceId): void
        {
            $i = 0;

            if ($this->trackingIDChargeDescription && $this->trackingIDChargeAmount) {
                foreach ($this->trackingIDChargeDescription as $key => $description) {
                    if ($description && !in_array($description, $charges)) {
                        $surchargeId = $this->getFedexSurchargeId($description, $invoiceId);
                        $this->trackingChargeData[$i] = [
                            'fedex_invoice_id' => $invoiceId,
                            'fedex_surcharge_name_id' => $surchargeId,
                            'description' => $description,
                            'amount' => $this->trackingIDChargeAmount[$key],
                            'created_at' => now(),
                            'updated_at' => now(),
                        ];
                        $i++;
                    }
                }
            }
        }

        /**
         * Inserts tracking charges into the database in chunks.
         */
        private function insertTrackingCharges(): void
        {
            if (!empty($this->trackingChargeData)) {
                FedexInvoiceCharge::insert($this->trackingChargeData);
            }
        }

        /**
         * Retrieves or creates a FedEx surcharge name and logs any new entries.
         *
         * @param mixed $item The surcharge name.
         * @param int $invoiceId ID of the associated FedEx invoice.
         * @return int The ID of the surcharge name.
         */
        private function getFedexSurchargeId(mixed $item, int $invoiceId): int
        {
            $cacheKey = 'fedex_surcharge_name_' . md5($item);

            return Cache::remember($cacheKey, now()->addWeek(), function () use ($item, $invoiceId) {
                $surchargeName = FedexSurchargeName::where('name', $item)->first();

                if (!$surchargeName) {
                    $surchargeName = FedexSurchargeName::create(['name' => $item]);

                    // Log the new surcharge name for manual linking
                    Log::channel('invoice_importing')->warning(
                        'New FedEx surcharge name created. Requires manual linking to the correct surcharge.',
                        ['fedex_invoice_id' => $invoiceId, 'surcharge_name' => $surchargeName->toArray()]
                    );

                    slack_message(
                        'fedex_surcharge_needs_update_' . $surchargeName->id,
                        sprintf(
                            'New FedEx Surcharge Name created for FedExInvoice ID %d. This requires manual linking at %s. Name: %s, ID: %d',
                            $invoiceId,
                            url('/admin/surcharges'),
                            $surchargeName->name,
                            $surchargeName->id
                        ),
                        'good'
                    );
                }

                return $surchargeName->id;
            });
        }
    }
