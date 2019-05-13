package lidar_test

import (
	"context"
	"errors"
	"time"

	"code.cloudfoundry.org/lager/lagertest"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/concourse/concourse/atc"
	"github.com/concourse/concourse/atc/creds/credsfakes"
	"github.com/concourse/concourse/atc/db"
	"github.com/concourse/concourse/atc/db/dbfakes"
	"github.com/concourse/concourse/atc/db/lock/lockfakes"
	"github.com/concourse/concourse/atc/lidar"
	"github.com/concourse/concourse/atc/resource/resourcefakes"
	"github.com/concourse/concourse/atc/worker/workerfakes"
)

type Checker interface {
	Run(context.Context) error
}

var _ = Describe("Checker", func() {
	var (
		err error
		ctx context.Context

		fakeSecrets             *credsfakes.FakeSecrets
		fakeResourceFactory     *dbfakes.FakeResourceFactory
		fakeResourceCheck       *dbfakes.FakeResourceCheck
		fakeResource            *dbfakes.FakeResource
		fakeResourceType        *dbfakes.FakeResourceType
		fakeResourceConfig      *dbfakes.FakeResourceConfig
		fakeResourceConfigScope *dbfakes.FakeResourceConfigScope
		fakeLock                *lockfakes.FakeLock
		fakeParentType          *dbfakes.FakeResourceType
		parentVersion           atc.Version
		fakePool                *workerfakes.FakePool
		fakeWorker              *workerfakes.FakeWorker
		fakeContainer           *workerfakes.FakeContainer
		fakeCheckFactory        *resourcefakes.FakeResourceFactory
		fakeCheckable           *resourcefakes.FakeResource
		checkVersion            atc.Version

		checker Checker
	)

	BeforeEach(func() {
		ctx = context.Background()

		fakeSecrets = new(credsfakes.FakeSecrets)
		fakeResourceFactory = new(dbfakes.FakeResourceFactory)
		fakeResourceCheck = new(dbfakes.FakeResourceCheck)
		fakeResource = new(dbfakes.FakeResource)
		fakeResourceType = new(dbfakes.FakeResourceType)
		fakeResourceConfig = new(dbfakes.FakeResourceConfig)
		fakeResourceConfigScope = new(dbfakes.FakeResourceConfigScope)
		fakeLock = new(lockfakes.FakeLock)
		fakeParentType = new(dbfakes.FakeResourceType)
		parentVersion = atc.Version{}
		fakePool = new(workerfakes.FakePool)
		fakeWorker = new(workerfakes.FakeWorker)
		fakeContainer = new(workerfakes.FakeContainer)
		fakeCheckFactory = new(resourcefakes.FakeResourceFactory)
		fakeCheckable = new(resourcefakes.FakeResource)
		checkVersion = atc.Version{}

		logger := lagertest.NewTestLogger("test")
		checker = lidar.NewChecker(
			logger,
			fakeResourceFactory,
			fakeCheckFactory,
			fakeSecrets,
			fakePool,
			"external.url",
		)
	})

	JustBeforeEach(func() {
		err = checker.Run(ctx)
	})

	Context("when fetching resource checks fails", func() {
		BeforeEach(func() {
			fakeResourceFactory.ResourceChecksReturns(nil, errors.New("nope"))
		})

		It("errors", func() {
			Expect(err).To(HaveOccurred())
		})
	})

	Context("when fetching resource checks succeeds", func() {
		BeforeEach(func() {
			fakeResourceFactory.ResourceChecksReturns([]db.ResourceCheck{fakeResourceCheck}, nil)
		})

		Context("when fetching resource for the check fails", func() {
			BeforeEach(func() {
				fakeResourceCheck.ResourceReturns(nil, errors.New("nope"))
			})

			It("errors", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when fetching resource for the check succeeds", func() {
			BeforeEach(func() {
				fakeResourceCheck.ResourceReturns(fakeResource, nil)
			})

			Context("when fetching resource types for the resource fails", func() {
				BeforeEach(func() {
					fakeResource.ResourceTypesReturns(nil, errors.New("nope"))
				})

				It("errors", func() {
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when fetching resource types for the resource succeeds", func() {
				BeforeEach(func() {
					fakeResource.ResourceTypesReturns([]db.ResourceType{fakeResourceType}, nil)
				})

				Context("resolving credential source fails", func() {
					BeforeEach(func() {
						fakeSecrets.GetReturns(nil, nil, false, errors.New("nope"))
					})

					It("errors", func() {
						Expect(err).To(HaveOccurred())
					})
				})

				Context("secrets can't be found", func() {
					BeforeEach(func() {
						fakeSecrets.GetReturns(nil, nil, false, nil)
					})

					It("errors", func() {
						Expect(err).To(HaveOccurred())
					})
				})

				Context("secrets are expired", func() {
					BeforeEach(func() {
						past := time.Now().Add(-1 * time.Second)
						fakeSecrets.GetReturns(nil, &past, true, nil)
					})

					It("errors", func() {
						Expect(err).To(HaveOccurred())
					})
				})

				Context("resolving credential source succeeds", func() {
					BeforeEach(func() {
						future := time.Now().Add(time.Hour)
						fakeSecrets.GetReturns("some-secret", &future, true, nil)
					})

					Context("updating the resource config scope fails", func() {
						BeforeEach(func() {
							fakeResource.SetResourceConfigReturns(nil, errors.New("nope"))
						})

						It("errors", func() {
							Expect(err).To(HaveOccurred())
						})
					})

					Context("updating the resource config scope succeeds", func() {
						BeforeEach(func() {
							fakeResource.SetResourceConfigReturns(fakeResourceConfigScope, nil)
							fakeResourceConfigScope.ResourceConfigReturns(fakeResourceConfig)
						})

						Context("acquiring the lock fails", func() {
							BeforeEach(func() {
								fakeResourceConfigScope.AcquireResourceCheckingLockReturns(nil, false, errors.New("nope"))
							})

							It("errors", func() {
								Expect(err).To(HaveOccurred())
							})
						})

						Context("lock is already held", func() {
							BeforeEach(func() {
								fakeResourceConfigScope.AcquireResourceCheckingLockReturns(nil, false, nil)
							})

							It("errors", func() {
								Expect(err).To(HaveOccurred())
							})
						})

						Context("acquiring the lock succeeds", func() {
							BeforeEach(func() {
								fakeResourceConfigScope.AcquireResourceCheckingLockReturns(fakeLock, true, nil)
							})

							Context("starting the check fails", func() {
								BeforeEach(func() {
									fakeResourceCheck.StartReturns(errors.New("nope"))
								})

								It("errors", func() {
									Expect(err).To(HaveOccurred())
								})
							})

							Context("starting the check succeeds", func() {
								BeforeEach(func() {
									fakeResourceCheck.StartReturns(nil)
								})

								Context("fetching the parent resource type fails", func() {
									BeforeEach(func() {
										fakeResource.ParentResourceTypeReturns(nil, errors.New("nope"))
									})

									It("errors", func() {
										Expect(err).To(HaveOccurred())
									})
								})

								Context("fetching the parent resource type succeeds", func() {
									BeforeEach(func() {
										fakeResource.ParentResourceTypeReturns(fakeParentType, nil)
									})

									Context("parent has no version", func() {
										BeforeEach(func() {
											fakeParentType.VersionReturns(nil)
										})

										It("errors", func() {
											Expect(err).To(HaveOccurred())
										})
									})

									Context("parent has a version", func() {
										BeforeEach(func() {
											fakeParentType.VersionReturns(parentVersion)
										})

										Context("choosing a worker fails", func() {
											BeforeEach(func() {
												fakePool.FindOrChooseWorkerForContainerReturns(nil, errors.New("nope"))
											})

											It("errors", func() {
												Expect(err).To(HaveOccurred())
											})
										})

										Context("choosing a worker succeeds", func() {
											BeforeEach(func() {
												fakePool.FindOrChooseWorkerForContainerReturns(fakeWorker, nil)
											})

											Context("creating a container fails", func() {
												BeforeEach(func() {
													fakeWorker.FindOrCreateContainerReturns(nil, errors.New("nope"))
												})

												It("errors", func() {
													Expect(err).To(HaveOccurred())
												})
											})

											Context("creating a container succeeds", func() {
												BeforeEach(func() {
													fakeWorker.FindOrCreateContainerReturns(fakeContainer, nil)
												})

												Context("converting the container to something checkable", func() {
													BeforeEach(func() {
														fakeCheckFactory.NewResourceForContainerReturns(fakeCheckable)
													})

													Context("checking fails", func() {
														BeforeEach(func() {
															fakeCheckable.CheckReturns(nil, errors.New("nope"))
														})

														It("errors", func() {
															Expect(err).To(HaveOccurred())
														})
													})

													Context("checking succeeds", func() {
														BeforeEach(func() {
															fakeCheckable.CheckReturns([]atc.Version{checkVersion}, nil)
														})

														Context("saving versions fails", func() {
															BeforeEach(func() {
																fakeResourceConfigScope.SaveVersionsReturns(errors.New("nope"))
															})

															It("errors", func() {
																Expect(err).To(HaveOccurred())
															})
														})

														Context("saving versions succeeds", func() {
															BeforeEach(func() {
																fakeResourceConfigScope.SaveVersionsReturns(nil)
															})

															Context("finishing a resource check fails", func() {
																BeforeEach(func() {
																	fakeResourceCheck.FinishReturns(errors.New("nope"))
																})

																It("errors", func() {
																	Expect(err).To(HaveOccurred())
																})
															})

															Context("finishing a resource checks succeeds", func() {
																BeforeEach(func() {
																	fakeResourceCheck.FinishReturns(nil)
																})

																It("succeeds", func() {
																	Expect(err).NotTo(HaveOccurred())
																})
															})
														})
													})
												})
											})
										})
									})
								})
							})
						})
					})
				})
			})
		})
	})
})
