<?php

    namespace App\Jobs\InvoiceImporting\Fedex;

    use App\Enums\InvoiceFileImportStatusEnum;
    use App\Models\FedexInvoice;
    use App\Models\FedexInvoiceCharge;
    use App\Models\InvoiceFile;
    use App\Models\InvoiceImportError;
    use Illuminate\Bus\Queueable;
    use Illuminate\Contracts\Queue\ShouldQueue;
    use Illuminate\Foundation\Bus\Dispatchable;
    use Illuminate\Queue\InteractsWithQueue;
    use Illuminate\Queue\SerializesModels;
    use Illuminate\Support\Facades\DB;
    use Illuminate\Support\Facades\Log;
    use Illuminate\Support\Facades\Storage;

    /**
     * Job to import FedEx invoice data into the system.
     * Handles retrieving, parsing, and processing of invoice files.
     */
    class ImportFedexInvoiceDataJob implements ShouldQueue
    {
        use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

        public array $tracking_data = []; // Stores tracking data for charges
        public int $i; // Counter for tracking data entries

        // Paths for file storage on S3
        public string $s3_pending_path = 'carrier_invoices/fedex/pending_import/';
        public string $s3_success_path = 'carrier_invoices/fedex/imported/';
        public string $s3_failed_path = 'carrier_invoices/fedex/error/';

        /**
         * Main handler for the job.
         * Processes pending FedEx invoice files.
         *
         * @return void
         */
        public function handle(): void
        {
            ini_set('memory_limit', '-1'); // Increase memory limit for large files
            Log::channel('invoice_importing')->info('***** Import FedEx Invoice Data Job Started *****');

            // Retrieve pending files from the database
            Log::channel('invoice_importing')->info('Fetching pending invoice files...');
            $pendingFiles = InvoiceFile::whereCarrier(config('carriers')->FEDEX->code)
                ->whereImportStatus(InvoiceFileImportStatusEnum::PROCESSING)
                ->get();

            Log::channel('invoice_importing')->info('Found ' . count($pendingFiles) . ' pending files.');

            // Process each pending file
            foreach ($pendingFiles as $file) {
                $this->processFile($file);
            }

            Log::channel('invoice_importing')->info('All pending files processed.');
            Log::channel('invoice_importing')->info('***** Import FedEx Invoice Data Job Completed *****');
        }

        /**
         * Process a single invoice file.
         *
         * @param mixed $file The invoice file to process.
         * @return void
         */
        private function processFile(mixed $file): void
        {
            Log::channel('invoice_importing')->info('Processing Invoice File: ' . $file->file_name);

            try {
                $this->tracking_data = [];
                $this->i = 0;

                // Check if the file exists in the S3 bucket
                if (Storage::disk('s3')->missing($this->s3_pending_path . $file->file_name)) {
                    $this->handleMissingFile($file);
                    return;
                }

                // Parse the XML content of the file
                Log::channel('invoice_importing')->info('Parsing XML content...');
                $invoice_arr = $this->parseXml(Storage::disk('s3')->get($this->s3_pending_path . $file->file_name));
                Log::channel('invoice_importing')->info('XML parsed successfully. Found ' . count($invoice_arr['Invoice_Download']) . ' invoices.');

                // Dispatch jobs to process each invoice. Our workers run in parallel to process multiple invoices.
                foreach ($invoice_arr['Invoice_Download'] as $value) {
                    ProcessFedexInvoiceJob::dispatch($value, $file->id)
                        ->onQueue('fedex_invoice_process-' . app()->environment());
                }

                Log::channel('invoice_importing')->info('All invoices dispatched for processing.');

                // Move the file to the success folder and update its status
                $this->moveFile($file);
            } catch (\Exception $exception) {
                $this->handleImportException($file, $exception);
            }
        }

        /**
         * Handle a missing file scenario by logging and updating the file status.
         *
         * @param mixed $file The missing invoice file.
         * @return void
         */
        private function handleMissingFile(mixed $file): void
        {
            $errorMessage = 'File not found: ' . $file->file_name;

            Log::channel('invoice_importing')->error($errorMessage);
            slack_message('ImportFedexInvoiceDataJob-1', $errorMessage);

            InvoiceImportError::create([
                'invoice_file_id' => $file->id,
                'error_type'      => 'fedex',
                'error'           => json_encode($errorMessage),
            ]);

            $file->update(['import_status' => InvoiceFileImportStatusEnum::FAILED]);
        }

        /**
         * Parse XML content and convert it to JSON for processing.
         *
         * @param string|null $xmlString The raw XML string.
         * @return mixed The parsed JSON data.
         */
        private function parseXml(?string $xmlString): mixed
        {
            $xmlObject = simplexml_load_string($xmlString);
            $json = str_replace(':{}', ':null', json_encode($xmlObject)); // Replace empty objects with null
            return json_decode($json, true);
        }

        /**
         * Move a successfully processed file to the "imported" folder.
         *
         * @param mixed $file The successfully processed invoice file.
         * @return void
         */
        private function moveFile(mixed $file): void
        {
            $successPath = $this->s3_success_path . $file->file_name;

            Storage::disk('s3')->move($this->s3_pending_path . $file->file_name, $successPath);

            $file->update(['import_status' => InvoiceFileImportStatusEnum::SUCCESS]);

            Log::channel('invoice_importing')->info('File moved to success path: ' . $successPath);
        }

        /**
         * Handle exceptions during file processing by logging and updating the file status.
         *
         * @param mixed $file The invoice file being processed.
         * @param \Exception $exception The exception that occurred.
         * @return void
         */
        private function handleImportException(mixed $file, \Exception $exception): void
        {
            $errorDetails = sprintf(
                'Error in file %s: %s in %s on line %d',
                $file->file_name,
                $exception->getMessage(),
                $exception->getFile(),
                $exception->getLine()
            );

            Log::channel('invoice_importing')->error($errorDetails);
            slack_message('ImportFedexInvoiceDataJob-2', $errorDetails);

            // Move the file to the failed folder
            Storage::disk('s3')->move($this->s3_pending_path . $file->file_name, $this->s3_failed_path . $file->file_name);

            $file->update(['import_status' => InvoiceFileImportStatusEnum::FAILED]);

            // Record the error in the database
            InvoiceImportError::create([
                'invoice_file_id' => $file->id,
                'error_type'      => 'fedex',
                'error'           => json_encode($exception->getMessage()),
            ]);
        }
    }
