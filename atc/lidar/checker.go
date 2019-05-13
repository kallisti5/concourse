package lidar

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"code.cloudfoundry.org/lager"
	"github.com/concourse/concourse/atc/creds"
	"github.com/concourse/concourse/atc/db"
	"github.com/concourse/concourse/atc/resource"
	"github.com/concourse/concourse/atc/worker"
)

var ErrFailedToAcquireLock = errors.New("failed to acquire lock")

func NewChecker(
	logger lager.Logger,
	resourceFactory db.ResourceFactory,
	checkFactory resource.ResourceFactory,
	secrets creds.Secrets,
	pool worker.Pool,
	externalURL string,
) *checker {
	return &checker{
		logger,
		resourceFactory,
		checkFactory,
		secrets,
		pool,
		externalURL,
	}
}

type checker struct {
	logger          lager.Logger
	resourceFactory db.ResourceFactory
	checkFactory    resource.ResourceFactory
	secrets         creds.Secrets
	pool            worker.Pool
	externalURL     string
}

func (c *checker) Run(ctx context.Context) error {

	resourceChecks, err := c.resourceFactory.ResourceChecks()
	if err != nil {
		c.logger.Error("failed-to-fetch-resource-checks", err)
		return err
	}

	waitGroup := new(sync.WaitGroup)

	for _, resourceCheck := range resourceChecks {
		go c.check(ctx, resourceCheck, waitGroup)
	}

	waitGroup.Wait()

	return nil
}

func (c *checker) check(ctx context.Context, resourceCheck db.ResourceCheck, waitGroup *sync.WaitGroup) error {
	waitGroup.Add(1)
	defer waitGroup.Done()

	if err := c.tryCheck(ctx, resourceCheck); err != nil {
		if err == ErrFailedToAcquireLock {
			return err
		}

		if err = resourceCheck.FinishWithError(err.Error()); err != nil {
			c.logger.Error("failed-to-udpate-resource-check-error", err)
			return err
		}
	}

	return nil
}

func (c *checker) tryCheck(ctx context.Context, resourceCheck db.ResourceCheck) error {

	resource, err := resourceCheck.Resource()
	if err != nil {
		c.logger.Error("failed-to-fetch-resource", err)
		return err
	}

	resourceTypes, err := resource.ResourceTypes()
	if err != nil {
		c.logger.Error("failed-to-fetch-resource-types", err)
		return err
	}

	variables := creds.NewVariables(c.secrets, resource.PipelineName(), resource.TeamName())

	source, err := creds.NewSource(variables, resource.Source()).Evaluate()
	if err != nil {
		c.logger.Error("failed-to-evaluate-source", err)
		return err
	}

	versionedResourceTypes := creds.NewVersionedResourceTypes(variables, resourceTypes.Deserialize())

	// This could have changed based on new variable interpolation so update it
	resourceConfigScope, err := resource.SetResourceConfig(source, versionedResourceTypes)
	if err != nil {
		c.logger.Error("failed-to-update-resource-config", err)
		return err
	}

	logger := c.logger.Session("check", lager.Data{
		"resource_id":        resource.ID(),
		"resource_name":      resource.Name(),
		"resource_config_id": resourceConfigScope.ResourceConfig().ID(),
	})

	lock, acquired, err := resourceConfigScope.AcquireResourceCheckingLock(logger)
	if err != nil {
		logger.Error("failed-to-get-lock", err)
		return ErrFailedToAcquireLock
	}

	if !acquired {
		logger.Debug("lock-not-acquired")
		return ErrFailedToAcquireLock
	}

	defer lock.Release()

	if err = resourceCheck.Start(); err != nil {
		logger.Error("failed-to-start-resource-check", err)
		return err
	}

	parent, err := resource.ParentResourceType()
	if err != nil {
		logger.Error("failed-to-fetch-parent-type", err)
		return err
	}

	if parent.Version() == nil {
		err = errors.New("parent resource has no version")
		logger.Error("failed-due-to-missing-parent-version", err)
		return err
	}

	container, err := c.createContainer(logger, ctx, resource, resourceConfigScope.ResourceConfig(), versionedResourceTypes)
	if err != nil {
		logger.Error("failed-to-create-resource-container", err)
		return err
	}

	deadline, cancel := context.WithTimeout(ctx, resourceCheck.Timeout())
	defer cancel()

	logger.Debug("checking", lager.Data{"from": resourceCheck.FromVersion()})

	checkable := c.checkFactory.NewResourceForContainer(container)
	versions, err := checkable.Check(deadline, source, resourceCheck.FromVersion())
	if err != nil {
		if err == context.DeadlineExceeded {
			return fmt.Errorf("Timed out after %v while checking for new versions", resourceCheck.Timeout())
		}
		return err
	}

	if err = resourceConfigScope.SaveVersions(versions); err != nil {
		logger.Error("failed-to-save-versions", err)
		return err
	}

	return resourceCheck.Finish()
}

func (c *checker) createContainer(
	logger lager.Logger,
	ctx context.Context,
	dbResource db.Resource,
	dbResourceConfig db.ResourceConfig,
	resourceTypes creds.VersionedResourceTypes,
) (worker.Container, error) {

	metadata := resource.TrackerMetadata{
		ResourceName: dbResource.Name(),
		PipelineName: dbResource.PipelineName(),
		ExternalURL:  c.externalURL,
	}

	containerSpec := worker.ContainerSpec{
		ImageSpec: worker.ImageSpec{
			ResourceType: dbResource.Type(),
		},
		BindMounts: []worker.BindMountSource{
			&worker.CertsVolumeMount{Logger: c.logger},
		},
		Tags:   dbResource.Tags(),
		TeamID: dbResource.TeamID(),
		Env:    metadata.Env(),
	}

	workerSpec := worker.WorkerSpec{
		ResourceType:  dbResource.Type(),
		Tags:          dbResource.Tags(),
		ResourceTypes: resourceTypes,
		TeamID:        dbResource.TeamID(),
	}

	owner := db.NewResourceConfigCheckSessionContainerOwner(
		dbResourceConfig,
		db.ContainerOwnerExpiries{
			GraceTime: 2 * time.Minute,
			Min:       5 * time.Minute,
			Max:       1 * time.Hour,
		},
	)

	containerMetadata := db.ContainerMetadata{
		Type: db.ContainerTypeCheck,
	}

	chosenWorker, err := c.pool.FindOrChooseWorkerForContainer(
		logger,
		owner,
		containerSpec,
		workerSpec,
		worker.NewRandomPlacementStrategy(),
	)
	if err != nil {
		return nil, err
	}

	return chosenWorker.FindOrCreateContainer(
		ctx,
		logger,
		worker.NoopImageFetchingDelegate{},
		owner,
		containerMetadata,
		containerSpec,
		resourceTypes,
	)
}
